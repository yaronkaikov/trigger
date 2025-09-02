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
            
        if pr.merged:
            count += 1
            # Check if PR has backport labels
            backport_labels = [label.name for label in pr.labels if backport_label_pattern.match(label.name)]
            if backport_labels:
                # Check if this is a backport PR (title starts with [Backport])
                if pr.title.startswith('[Backport'):
                    # For waterfall, we only need the most recent merged backport PR that still has backport labels
                    # Return it immediately
                    logging.info(f"Found merged backport PR #{pr.number} with labels: {backport_labels}")
                    return [(pr, backport_labels)]
            
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
            head=f'scylladbbot:{new_branch_name}',
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


def backport(repo, pr, version, commits, backport_base_branch):
    new_branch_name = f'backport/{pr.number}/to-{version}'
    backport_pr_title = f'[Backport {version}] {pr.title}'
    repo_url = f'https://scylladbbot:{github_token}@github.com/{repo.full_name}.git'
    fork_repo = f'https://scylladbbot:{github_token}@github.com/scylladbbot/{repo.name}.git'
    with (tempfile.TemporaryDirectory() as local_repo_path):
        try:
            repo_local = Repo.clone_from(repo_url, local_repo_path, branch=backport_base_branch)
            repo_local.git.checkout(b=new_branch_name)
            is_draft = False
            for commit in commits:
                try:
                    repo_local.git.cherry_pick(commit, '-m1', '-x')
                except GitCommandError as e:
                    logging.warning(f'Cherry-pick conflict on commit {commit}: {e}')
                    is_draft = True
                    repo_local.git.add(A=True)
                    repo_local.git.cherry_pick('--continue')
            repo_local.git.push(fork_repo, new_branch_name, force=True)
            create_pull_request(repo, new_branch_name, backport_base_branch, pr, backport_pr_title, commits,
                                is_draft=is_draft)
        except GitCommandError as e:
            logging.warning(f"GitCommandError: {e}")


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


def waterfall_backport(repo, pr, sorted_backport_labels, commits, backport_branch_prefix):
    """
    Implements a waterfall backporting process.
    Creates a backport PR for the latest version first, adding all remaining backport labels to it.
    When that PR is merged, the next version in the waterfall will be handled.
    
    Example: If PR has labels backport/2025.3, backport/2025.2, and backport/2024.1
    1. First backport to 2025.3, adding remaining labels to that PR
    2. When that PR is merged, the script will be run again to backport to 2025.2
    3. And so on
    """
    if not sorted_backport_labels:
        return
    
    current_label = sorted_backport_labels[0]  # Get the latest version
    remaining_labels = sorted_backport_labels[1:]  # Get the remaining versions for future backports
    
    version = current_label.replace('backport/', '')
    backport_base_branch = current_label.replace('backport/', backport_branch_prefix)
    
    logging.info(f"Starting waterfall backport to {version}")
    
    # Create the backport PR
    new_branch_name = f'backport/{pr.number}/to-{version}'
    backport_pr_title = f'[Backport {version}] {pr.title}'
    repo_url = f'https://scylladbbot:{github_token}@github.com/{repo.full_name}.git'
    fork_repo = f'https://scylladbbot:{github_token}@github.com/scylladbbot/{repo.name}.git'
    
    with tempfile.TemporaryDirectory() as local_repo_path:
        try:
            repo_local = Repo.clone_from(repo_url, local_repo_path, branch=backport_base_branch)
            repo_local.git.checkout(b=new_branch_name)
            is_draft = False
            for commit in commits:
                try:
                    repo_local.git.cherry_pick(commit, '-m1', '-x')
                except GitCommandError as e:
                    logging.warning(f'Cherry-pick conflict on commit {commit}: {e}')
                    is_draft = True
                    repo_local.git.add(A=True)
                    repo_local.git.cherry_pick('--continue')
            repo_local.git.push(fork_repo, new_branch_name, force=True)
            
            # Create PR
            backport_pr = create_pull_request(repo, new_branch_name, backport_base_branch, pr, backport_pr_title, commits, is_draft=is_draft)
            
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
                
                backport_pr.create_issue_comment(pr_comment)
                
        except GitCommandError as e:
            logging.warning(f"GitCommandError: {e}")


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
        
        for pr, backport_labels in merged_prs_with_labels:
            logging.info(f"Found merged backport PR #{pr.number} with backport labels: {backport_labels}")
            
            # Get the original PR number from the backport PR
            parent_pr_match = re.search(r'Parent PR: #(\d+)', pr.body)
            if parent_pr_match:
                original_pr_number = int(parent_pr_match.group(1))
                logging.info(f"Found original PR #{original_pr_number}")
                
                try:
                    original_pr = repo.get_pull(original_pr_number)
                    
                    # Check if original PR has "backport_all" label
                    original_labels = [label.name for label in original_pr.labels]
                    has_backport_all = any(label == 'backport_all' for label in original_labels)
                    
                    # Extract the current version that was just processed
                    current_version_match = re.search(r'\[Backport ([\d\.]+)\]', pr.title)
                    if current_version_match:
                        processed_version = current_version_match.group(1)
                        logging.info(f"Processed version: {processed_version}")
                        
                        # Sort remaining labels by version
                        sorted_backport_labels = sort_versions(backport_labels)
                        
                        # Get commits from the original PR
                        commits = get_pr_commits(repo, original_pr, stable_branch)
                        
                        if has_backport_all and args.parallel:
                            logging.info(f"PR #{original_pr_number} has 'backport_all' label, doing parallel backports")
                            # Do parallel backports for remaining labels
                            for backport_label in sorted_backport_labels:
                                version = backport_label.replace('backport/', '')
                                backport_base_branch = backport_label.replace('backport/', backport_branch)
                                backport(repo, original_pr, version, commits, backport_base_branch)
                        else:
                            # Continue the waterfall with remaining labels
                            waterfall_backport(repo, original_pr, sorted_backport_labels, commits, backport_branch)
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
        # 3. If PR has backport_all label, do parallel backports
        # 4. Otherwise, default to waterfall backports
        if args.parallel or (has_backport_all and not args.waterfall):
            logging.info(f"Using parallel approach for PR #{pr.number} (backport_all label found: {has_backport_all})")
            # Original approach - backport to all versions in parallel
            for backport_label in sorted_backport_labels:
                version = backport_label.replace('backport/', '')
                backport_base_branch = backport_label.replace('backport/', backport_branch)
                backport(repo, pr, version, commits, backport_base_branch)
        else:
            # Use waterfall approach
            logging.info(f"Using waterfall approach for PR #{pr.number}")
            waterfall_backport(repo, pr, sorted_backport_labels, commits, backport_branch)


if __name__ == "__main__":
    main()
