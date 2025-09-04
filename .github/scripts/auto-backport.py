#!/usr/bin/env python3

import argparse
import os
import re
import sys
import tempfile
import logging

from github import Github, GithubException
from git import Repo, GitCommandError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
try:
    github_token = os.environ["GITHUB_TOKEN"]
except KeyError:
    print("Please set the 'GITHUB_TOKEN' environment variable")
    sys.exit(1)


def is_pull_request():
    return '--pull-request' in sys.argv[1:]


def check_pr_merged(repo, pr_number):
    """
    Check if a PR is merged, and if so, return the PR object.
    """
    try:
        pr = repo.get_pull(pr_number)
        if pr.merged:
            logging.info(f"PR #{pr_number} is merged")
            return pr
        else:
            logging.info(f"PR #{pr_number} is not merged yet")
            return None
    except GithubException as e:
        logging.error(f"Error checking PR #{pr_number}: {e}")
        return None
        
        
def find_merged_prs_with_labels(repo, backport_label_pattern):
    """
    Find merged PRs with backport labels.
    Used for waterfall backporting to find PRs that need to be backported to the next version.
    Returns a list of tuples (pr, backport_labels).
    
    For the waterfall process, we only need the most recently merged backport PR that still has
    backport labels.
    """
    # Get recently merged PRs (last 20)
    merged_prs = repo.get_pulls(state='closed', sort='updated', direction='desc')
    count = 0
    
    for pr in merged_prs:
        # Limit to last 20 PRs to avoid API rate limits
        if count >= 20:
            break
            
        if pr.state == 'closed' and pr.merged:  # Make sure PR is both closed and merged
            count += 1
            logging.info(f"Checking PR #{pr.number} ({count}/20): {pr.title}")
            
            # Check if PR has backport labels
            backport_labels = [label.name for label in pr.labels if backport_label_pattern.match(label.name)]
            if backport_labels:
                # Check if this is a backport PR (title starts with [Backport])
                if pr.title.startswith('[Backport'):
                    # For waterfall, we only need the most recent merged backport PR that still has backport labels
                    # Return it immediately
                    logging.info(f"Found merged backport PR #{pr.number} with labels: {backport_labels}")
                    return [(pr, backport_labels)]
            
    logging.info("No merged backport PRs with remaining backport labels found")
    return []


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repo', type=str, required=True, help='Github repository name')
    parser.add_argument('--base-branch', type=str, default='refs/heads/next', help='Base branch')
    parser.add_argument('--commits', default=None, type=str, help='Range of promoted commits.')
    parser.add_argument('--pull-request', type=int, help='Pull request number to be backported')
    parser.add_argument('--head-commit', type=str, required=is_pull_request(), help='The HEAD of target branch after the pull request specified by --pull-request is merged')
    parser.add_argument('--label', type=str, required=is_pull_request(), help='Backport label name when --pull-request is defined')
    parser.add_argument('--waterfall', action='store_true', help='Force waterfall backporting process')
    parser.add_argument('--parallel', action='store_true', help='Force parallel backporting process')
    return parser.parse_args()


def create_pull_request(repo, new_branch_name, base_branch_name, pr, backport_pr_title, commits, is_draft=False):
    pr_body = f'{pr.body}\n\n'
    for commit in commits:
        pr_body += f'- (cherry picked from commit {commit})\n\n'
    pr_body += f'Parent PR: #{pr.number}'
    try:
        backport_pr = repo.create_pull(
            title=backport_pr_title,
            body=pr_body,
            head=f'yaronkaikov:{new_branch_name}',
            base=base_branch_name,
            draft=is_draft
        )
        logging.info(f"Pull request created: {backport_pr.html_url}")
        backport_pr.add_to_assignees(pr.user)
        if is_draft:
            backport_pr.add_to_labels("conflicts")
            pr_comment = f"@{pr.user.login} - This PR has conflicts, therefore it was moved to `draft` \n"
            pr_comment += "Please resolve them and mark this PR as ready for review"
            backport_pr.create_issue_comment(pr_comment)
        logging.info(f"Assigned PR to original author: {pr.user}")
        return backport_pr
    except GithubException as e:
        if 'A pull request already exists' in str(e):
            logging.warning(f'A pull request already exists for {pr.user}:{new_branch_name}')
        else:
            logging.error(f'Failed to create PR: {e}')


def get_pr_commits(repo, pr, stable_branch, start_commit=None):
    commits = []
    if pr.merged:
        merge_commit = repo.get_commit(pr.merge_commit_sha)
        if len(merge_commit.parents) > 1:  # Check if this merge commit includes multiple commits
            commits.append(pr.merge_commit_sha)
        else:
            if start_commit:
                promoted_commits = repo.compare(start_commit, stable_branch).commits
            else:
                promoted_commits = repo.get_commits(sha=stable_branch)
            for commit in pr.get_commits():
                for promoted_commit in promoted_commits:
                    commit_title = commit.commit.message.splitlines()[0]
                    # In Scylla-pkg and scylla-dtest, for example,
                    # we don't create a merge commit for a PR with multiple commits,
                    # according to the GitHub API, the last commit will be the merge commit,
                    # which is not what we need when backporting (we need all the commits).
                    # So here, we are validating the correct SHA for each commit so we can cherry-pick
                    if promoted_commit.commit.message.startswith(commit_title):
                        commits.append(promoted_commit.sha)

    elif pr.state == 'closed':
        events = pr.get_issue_events()
        for event in events:
            if event.event == 'closed':
                commits.append(event.commit_id)
    return commits


def backport(repo, pr, sorted_backport_labels, commits, backport_branch_prefix, use_waterfall=True):
    """   
    Unified backport function that handles both waterfall and parallel backporting strategies.
    
    Args:
        repo: GitHub repository object
        pr: Pull request object to backport
        sorted_backport_labels: List of backport labels sorted by version (newest first)
        commits: List of commits to cherry-pick
        backport_branch_prefix: Prefix for the backport branch (e.g., 'next-')
        use_waterfall: If True, use waterfall strategy; otherwise, use parallel strategy
        
    Returns:
        List of created PR objects
    """
    if not sorted_backport_labels:
        logging.info("No backport labels provided")
        return []
    
    created_prs = []
    
    if use_waterfall:
        # Waterfall strategy: Backport to latest version first with remaining labels attached
        logging.info(f"Using waterfall approach for PR #{pr.number}")
        
        current_label = sorted_backport_labels[0]  # Get the latest version
        remaining_labels = sorted_backport_labels[1:]  # Get the remaining versions for future backports
        
        version = current_label.replace('backport/', '')
        
        logging.info(f"Starting waterfall backport to {version}")
        
        # Create the backport PR using our helper function
        # In waterfall mode, we ensure to pass the full commits list to include all changes
        # including new files that might have been added
        backport_pr = create_backport_branch(
            repo=repo,
            pr=pr,
            version=version,
            commits=commits,
            backport_branch_prefix=backport_branch_prefix,
            remaining_labels=remaining_labels
        )
        
        if backport_pr and remaining_labels:
            # Add remaining backport labels to the PR for the next backports in the waterfall
            for label in remaining_labels:
                try:
                    backport_pr.add_to_labels(label)
                    logging.info(f"Added label {label} to PR #{backport_pr.number}")
                except Exception as e:
                    logging.error(f"Failed to add label {label} to PR #{backport_pr.number}: {e}")
            
            # Add comment explaining the waterfall process
            pr_comment = f"@{pr.user.login} - This PR needs to be backported to the following versions after merging:\n"
            for label in remaining_labels:
                version_label = label.replace('backport/', '')
                pr_comment += f"- {version_label}\n\n"
            
            # Add explanation about automated waterfall backporting
            pr_comment += f"The GitHub workflow will automatically continue the backport process to these versions after this PR is merged."
            
            try:
                backport_pr.create_issue_comment(pr_comment)
                logging.info(f"Added explanation comment to PR #{backport_pr.number}")
            except Exception as e:
                logging.error(f"Failed to add comment to PR #{backport_pr.number}: {e}")
            
        if backport_pr:
            created_prs.append(backport_pr)
            logging.info(f"Successfully created backport PR #{backport_pr.number} for version {version}")
        else:
            logging.warning(f"Failed to create backport PR for version {version} - no PR was returned")
    else:
        # Parallel strategy: Backport to all versions at once
        logging.info(f"Using parallel approach for PR #{pr.number}")
        
        for backport_label in sorted_backport_labels:
            version = backport_label.replace('backport/', '')
            
            logging.info(f"Backporting PR #{pr.number} to version {version}")
            
            try:
                # Create backport PR for this version
                backport_pr = create_backport_branch(
                    repo=repo,
                    pr=pr,
                    version=version,
                    commits=commits,
                    backport_branch_prefix=backport_branch_prefix
                )
                
                if backport_pr:
                    created_prs.append(backport_pr)
                    logging.info(f"Successfully created backport PR #{backport_pr.number} for version {version}")
                else:
                    logging.warning(f"No PR created for version {version} (possibly no changes detected)")
            except Exception as e:
                logging.error(f"Error backporting to version {version}: {e}")
                logging.exception(e)  # Log full stack trace for debugging
    
    return created_prs


def create_pr_comment_and_remove_label(pr):
    comment_body = f':warning:  @{pr.user.login} PR body does not contain a valid reference to an issue '
    comment_body += ' based on [linking-a-pull-request-to-an-issue](https://docs.github.com/en/issues/tracking-your-work-with-issues/using-issues/linking-a-pull-request-to-an-issue#linking-a-pull-request-to-an-issue-using-a-keyword)'
    comment_body += ' and can not be backported\n\n'
    comment_body += 'The following labels were removed:\n'
    labels = pr.get_labels()
    pattern = re.compile(r"backport/\d+\.\d+$")
    for label in labels:
        if pattern.match(label.name):
            print(f"Removing label: {label.name}")
            comment_body += f'- {label.name}\n'
            pr.remove_from_labels(label)
    comment_body += f'\nPlease add the relevant backport labels after PR body is fixed'
    pr.create_issue_comment(comment_body)


def sort_versions(backport_labels):
    """
    Sort backport labels by version numbers in descending order (newest first).
    Example: ['backport/5.2', 'backport/5.0', 'backport/4.6'] -> ['backport/5.2', 'backport/5.0', 'backport/4.6']
    """
    def version_key(label):
        # Extract version from label (e.g., 'backport/5.2' -> '5.2')
        version = label.replace('backport/', '')
        # Split version into components and convert to float for proper sorting
        try:
            parts = version.split('.')
            if len(parts) == 1:
                return (float(parts[0]), 0)
            return (float(parts[0]), float(parts[1]))
        except ValueError:
            return (0, 0)  # Default for non-numeric versions
    
    return sorted(backport_labels, key=version_key, reverse=True)


def setup_git_repo(repo_url, fork_repo, backport_base_branch, new_branch_name, commits):
    """
    Helper function to set up a local Git repository and cherry-pick commits.
    
    Args:
        repo_url: URL of the source repository
        fork_repo: URL of the fork repository to push changes to
        backport_base_branch: Base branch to create the backport from
        new_branch_name: Name of the new branch to create
        commits: List of commits to cherry-pick
        
    Returns:
        tuple: (local_repo_path, is_draft) where local_repo_path is the path to the local repo
               and is_draft indicates if there were conflicts during cherry-picking
    """
    local_repo_path = tempfile.mkdtemp()
    is_draft = False
    
    try:
        # Clone the repository and set up the new branch
        repo_local = Repo.clone_from(repo_url, local_repo_path, branch=backport_base_branch)
        repo_local.git.checkout(b=new_branch_name)
        
        for commit in commits:
            try:
                # Use -x to add the original commit SHA in the message
                # Use -m1 to pick the first parent's changes when cherry-picking a merge commit
                # This is crucial for waterfall backporting where we're cherry-picking merge commits
                repo_local.git.cherry_pick(commit, '-m1', '-x')
            except GitCommandError as e:
                logging.warning(f'Cherry-pick conflict on commit {commit}: {e}')
                is_draft = True
                
                # In case of conflicts, add all changes including untracked files
                # This ensures new files added in previous commits are included
                repo_local.git.add(A=True)
                
                try:
                    # Try to continue the cherry-pick
                    repo_local.git.cherry_pick('--continue')
                except GitCommandError as continue_error:
                    # If cherry-pick --continue fails, check if it's because there are no changes
                    # (common when cherry-picking merge commits that don't have actual changes)
                    if "nothing to commit" in str(continue_error):
                        logging.info(f"No changes to commit for {commit}, skipping")
                        repo_local.git.cherry_pick('--skip')
                    else:
                        # For any other error, abort and mark as draft
                        logging.error(f"Failed to continue cherry-pick: {continue_error}")
                        repo_local.git.cherry_pick('--abort')
                        break
        
        # Push the changes to the fork repository
        repo_local.git.push(fork_repo, new_branch_name, force=True)
        return local_repo_path, is_draft
    
    except GitCommandError as e:
        logging.warning(f"GitCommandError during setup_git_repo: {e}")
        return local_repo_path, True  # Return as draft if there was an error


def create_backport_branch(repo, pr, version, commits, backport_branch_prefix, remaining_labels=None):
    """
    Creates a backport branch for a PR and cherry-picks the commits.
    
    Args:
        repo: GitHub repository object
        pr: Pull request object to backport
        version: Version to backport to (e.g., '5.2')
        commits: List of commits to cherry-pick
        backport_branch_prefix: Prefix for the backport branch (e.g., 'next-')
        remaining_labels: List of remaining labels for waterfall backporting
        
    Returns:
        The created PR object or None if creation failed
    """
    new_branch_name = f'backport/{pr.number}/to-{version}'
    # First create the regex pattern and then use it in the f-string to avoid backslash issues
    title_pattern = re.compile(r'^\[Backport [\d\.]+\]\s*')
    clean_title = re.sub(title_pattern, '', pr.title)
    backport_pr_title = f"[Backport {version}] {clean_title}"
    backport_base_branch = f"{backport_branch_prefix}{version}"
    
    repo_url = f'https://yaronkaikov:{github_token}@github.com/{repo.full_name}.git'
    fork_repo = f'https://yaronkaikov:{github_token}@github.com/yaronkaikov/{repo.name}.git'
    
    try:
        local_repo_path, is_draft = setup_git_repo(
            repo_url, 
            fork_repo, 
            backport_base_branch, 
            new_branch_name, 
            commits
        )
        
        # Check if the branch has any changes compared to the target branch
        # If not, don't create a PR as it would be empty
        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                diff_repo = Repo.clone_from(repo_url, temp_dir, branch=backport_base_branch)
                diff_repo.git.remote('add', 'fork', fork_repo)
                diff_repo.git.fetch('fork', new_branch_name)
                
                # Get the diff between target branch and our new branch
                # Include both changed files and new files in the diff
                diff = diff_repo.git.diff(f'{backport_base_branch}..fork/{new_branch_name}', name_only=True)
                
                if not diff.strip():
                    logging.info(f"No changes detected for version {version}, skipping PR creation")
                    return None
                else:
                    logging.info(f"Changes detected for version {version}: {diff}")
            except GitCommandError as e:
                logging.warning(f"Error checking for changes: {e}")
                # Continue anyway to be safe
        
        backport_pr = create_pull_request(
            repo, 
            new_branch_name, 
            backport_base_branch, 
            pr, 
            backport_pr_title, 
            commits, 
            is_draft=is_draft
        )
        
        return backport_pr
    
    except Exception as e:
        logging.error(f"Error creating backport branch: {e}")
        return None
    finally:
        # Clean up temp directory if it exists
        if 'local_repo_path' in locals() and os.path.exists(local_repo_path):
            import shutil
            shutil.rmtree(local_repo_path, ignore_errors=True)



def main():
    args = parse_args()
    base_branch = args.base_branch.split('/')[2]
    promoted_label = 'promoted-to-master'
    repo_name = args.repo

    backport_branch = 'next-'
    stable_branch = 'master' if base_branch == 'next' else base_branch.replace('next', 'branch')
    backport_label_pattern = re.compile(r'backport/\d+\.\d+$')

    g = Github(github_token)
    repo = g.get_repo(repo_name)
    
    # If no specific PR is requested, look for merged PRs with backport labels to continue the waterfall process
    if not args.pull_request and not args.parallel:
        logging.info("Checking for merged PRs with backport labels to continue waterfall process...")
        merged_prs_with_labels = find_merged_prs_with_labels(repo, backport_label_pattern)
        if merged_prs_with_labels:
            for pr, backport_labels in merged_prs_with_labels:
                logging.info(f"Found merged backport PR #{pr.number} with backport labels: {backport_labels}")
                
                # In waterfall backporting, we should cherry-pick from the last backport PR
                try:
                    # Check if the PR has been merged
                    if pr.merged:
                        # Extract the current version that was just processed
                        current_version_match = re.search(r'\[Backport ([\d\.]+)\]', pr.title)
                        if current_version_match:
                            processed_version = current_version_match.group(1)
                            logging.info(f"Processed version: {processed_version}")
                            
                            # Sort remaining labels by version
                            sorted_backport_labels = sort_versions(backport_labels)
                            
                            # Get the merge commit from the merged backport PR
                            # This includes all conflict resolutions made during the previous backport
                            commits = []
                            if pr.merge_commit_sha:
                                commits = [pr.merge_commit_sha]
                                logging.info(f"Using merge commit from backport PR: {pr.merge_commit_sha}")
                            else:
                                # Fallback in case there's no merge commit
                                logging.warning(f"No merge commit found for PR #{pr.number}, getting PR commits")
                                commits = [commit.sha for commit in pr.get_commits()]
                                
                            if commits:
                                logging.info(f"Found {len(commits)} commits for backporting to next version: {commits}")
                            else:
                                logging.warning(f"No commits found for backporting. This may cause the backport to fail.")
                                
                            # For UI display, also get the original PR information
                            parent_pr_match = re.search(r'Parent PR: #(\d+)', pr.body)
                            has_backport_all = False  # Default value
                            
                            if parent_pr_match:
                                original_pr_number = int(parent_pr_match.group(1))
                                logging.info(f"Original PR was #{original_pr_number}")
                                try:
                                    original_pr = repo.get_pull(original_pr_number)
                                    # Check if original PR has "backport_all" label
                                    original_labels = [label.name for label in original_pr.labels]
                                    has_backport_all = any(label == 'backport_all' for label in original_labels)
                                except Exception as e:
                                    logging.error(f"Error fetching original PR #{original_pr_number}: {e}")
                                    original_pr = pr  # Fallback to using the backport PR
                            else:
                                original_pr = pr

                            # Use parallel approach if PR has backport_all label, waterfall otherwise
                            use_waterfall = not has_backport_all
                            if has_backport_all:
                                logging.info(f"PR #{original_pr_number} has 'backport_all' label, doing parallel backports")
                            
                            # Call unified backport function with the appropriate strategy
                            backport(
                                repo=repo,
                                pr=original_pr,
                                sorted_backport_labels=sorted_backport_labels,
                                commits=commits,
                                backport_branch_prefix=backport_branch,
                                use_waterfall=use_waterfall
                            )
                except Exception as e:
                    logging.error(f"Error processing PR #{pr.number}: {e}")
            
            # Exit after processing waterfall backports
            return
    
    closed_prs = []
    start_commit = None

    if args.commits:
        start_commit, end_commit = args.commits.split('..')
        commits = repo.compare(start_commit, end_commit).commits
        for commit in commits:
            for pr in commit.get_pulls():
                closed_prs.append(pr)
    if args.pull_request:
        start_commit = args.head_commit
        pr = repo.get_pull(args.pull_request)
        closed_prs = [pr]

    for pr in closed_prs:
        labels = [label.name for label in pr.labels]
        if args.pull_request:
            backport_labels = [args.label]
        else:
            backport_labels = [label for label in labels if backport_label_pattern.match(label)]
        if promoted_label not in labels:
            print(f'no {promoted_label} label: {pr.number}')
            continue
        if not backport_labels:
            print(f'no backport label: {pr.number}')
            continue
        
        # Sort backport labels by version numbers (descending)
        sorted_backport_labels = sort_versions(backport_labels)
        logging.info(f"Sorted backport labels (newest first): {sorted_backport_labels}")
        
        commits = get_pr_commits(repo, pr, stable_branch, start_commit)
        logging.info(f"Found PR #{pr.number} with commits {commits} and the following labels: {sorted_backport_labels}")
        
        # Check if PR has "backport_all" label for parallel backporting
        has_backport_all = any(label == 'backport_all' for label in labels)
        
        # Determine backport strategy:
        # 1. If --parallel flag is specified, always do parallel backports
        # 2. If --waterfall flag is specified, always do waterfall backports
        # 3. If PR has backport_all label and --waterfall is not specified, do parallel backports
        # 4. Otherwise, default to waterfall backports
        use_waterfall = not (args.parallel or (has_backport_all and not args.waterfall))
        
        # Call the unified backport function with the appropriate strategy
        backport(
            repo=repo,
            pr=pr,
            sorted_backport_labels=sorted_backport_labels,
            commits=commits,
            backport_branch_prefix=backport_branch,
            use_waterfall=use_waterfall
        )


if __name__ == "__main__":
    main()
