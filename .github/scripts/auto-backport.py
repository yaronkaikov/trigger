#!/usr/bin/env python3

"""
Auto-backport script for ScyllaDB

This script implements cascading backports to reduce merge conflicts:
- When a PR has multiple backport labels (e.g., backport/2025.3, backport/2025.2, backport/2025.1)
- Only the newest version (2025.3) backport PR is created initially
- The remaining backport labels (2025.2, 2025.1) are added to the new backport PR
- When the newest backport PR is promoted, the process repeats for the next version
- This ensures backports cascade from newer to older releases, minimizing conflicts
- When a backport PR is successfully promoted/merged, a "done" label (e.g., backport/2025.3-done) 
  is automatically added to the original PR to track completion status
"""

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


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repo', type=str, required=True, help='Github repository name')
    parser.add_argument('--base-branch', type=str, default='refs/heads/next', help='Base branch')
    parser.add_argument('--commits', default=None, type=str, help='Range of promoted commits.')
    parser.add_argument('--pull-request', type=int, help='Pull request number to be backported')
    parser.add_argument('--head-commit', type=str, required=is_pull_request(), help='The HEAD of target branch after the pull request specified by --pull-request is merged')
    parser.add_argument('--label', type=str, required=is_pull_request(), help='Backport label name when --pull-request is defined')
    return parser.parse_args()


def create_pull_request(repo, new_branch_name, base_branch_name, pr, backport_pr_title, commits, remaining_backport_labels=None, current_version=None, is_draft=False):
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
        
        # Add remaining backport labels to the new PR for cascading backports
        if remaining_backport_labels:
            for label in remaining_backport_labels:
                try:
                    backport_pr.add_to_labels(label)
                    logging.info(f"Added label {label} to backport PR #{backport_pr.number}")
                except GithubException as e:
                    logging.warning(f"Failed to add label {label}: {e}")
        
        # Add version-specific promotion label for cascading backports
        # For backport PRs, add promoted-to-{version} instead of promoted-to-master
        if current_version:
            version_promotion_label = f"promoted-to-{current_version}"
            try:
                backport_pr.add_to_labels(version_promotion_label)
                logging.info(f"Added {version_promotion_label} label to backport PR #{backport_pr.number}")
            except GithubException as e:
                logging.warning(f"Failed to add {version_promotion_label} label: {e}")
        
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


def backport(repo, pr, version, commits, backport_base_branch, remaining_backport_labels=None):
    new_branch_name = f'backport/{pr.number}/to-{version}'
    backport_pr_title = f'[Backport {version}] {pr.title}'
    repo_url = f'https://yaronkaikov:{github_token}@github.com/{repo.full_name}.git'
    fork_repo = f'https://yaronkaikov:{github_token}@github.com/yaronkaikov/{repo.name}.git'
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
                                remaining_backport_labels, version, is_draft=is_draft)
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


def sort_backport_labels_descending(backport_labels):
    """
    Sort backport labels in descending order (newest version first).
    Example: ['backport/2025.1', 'backport/2025.3', 'backport/2025.2'] 
    becomes ['backport/2025.3', 'backport/2025.2', 'backport/2025.1']
    """
    def extract_version(label):
        # Extract version from backport/YYYY.X format
        match = re.search(r'backport/(\d+)\.(\d+)', label)
        if match:
            year, minor = match.groups()
            return (int(year), int(minor))
        return (0, 0)  # fallback for malformed labels
    
    return sorted(backport_labels, key=extract_version, reverse=True)


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
            # For --pull-request mode, get all backport labels from the actual PR
            # instead of just using the single --label argument
            backport_labels = [label for label in labels if backport_label_pattern.match(label)]
            # If no backport labels found on PR, fall back to the --label argument
            if not backport_labels and args.label:
                backport_labels = [args.label]
        else:
            backport_labels = [label for label in labels if backport_label_pattern.match(label)]
        
        # Check if this is a backport PR by looking at the title pattern
        is_backport_pr = pr.title.startswith('[Backport ') and '] ' in pr.title
        
        # For backport PRs, look for version-specific promotion labels
        # For original PRs, look for the standard promoted-to-master label
        has_promotion_label = False
        if is_backport_pr:
            # Extract version from title and look for promoted-to-{version} label
            title_match = re.search(r'\[Backport ([^\]]+)\]', pr.title)
            if title_match:
                backport_version = title_match.group(1)
                version_promotion_label = f"promoted-to-{backport_version}"
                has_promotion_label = version_promotion_label in labels
                logging.info(f"Looking for {version_promotion_label} label on backport PR #{pr.number}: {'found' if has_promotion_label else 'not found'}")
        else:
            # Original PR - look for promoted-to-master
            has_promotion_label = promoted_label in labels
        
        if not has_promotion_label:
            print(f'no promotion label: {pr.number}')
            continue
        if not backport_labels:
            print(f'no backport label: {pr.number}')
            continue
        
        commits = get_pr_commits(repo, pr, stable_branch, start_commit)
        logging.info(f"Found PR #{pr.number} with commit {commits} and the following labels: {backport_labels}")
        
        if is_backport_pr:
            # Extract the version from the title: "[Backport 2025.3] Some title"
            title_match = re.search(r'\[Backport ([^\]]+)\]', pr.title)
            if title_match:
                completed_version = title_match.group(1)
                
                # Find the original PR by looking for "Parent PR: #" in the body
                parent_pr_match = re.search(r'Parent PR: #(\d+)', pr.body)
                if parent_pr_match:
                    parent_pr_number = int(parent_pr_match.group(1))
                    try:
                        parent_pr = repo.get_pull(parent_pr_number)
                        done_label = f"backport/{completed_version}-done"
                        try:
                            parent_pr.add_to_labels(done_label)
                            logging.info(f"Added {done_label} label to original PR #{parent_pr.number}")
                        except GithubException as e:
                            logging.warning(f"Failed to add {done_label} label to original PR: {e}")
                    except GithubException as e:
                        logging.warning(f"Failed to get parent PR #{parent_pr_number}: {e}")
        
        # Sort backport labels in descending order (newest version first)
        sorted_backport_labels = sort_backport_labels_descending(backport_labels)
        logging.info(f"Sorted backport labels: {sorted_backport_labels}")
        
        # For cascading backports, only process the newest version
        # and add the remaining labels to the new backport PR
        if len(sorted_backport_labels) > 1:
            # Only create backport for the newest version
            newest_backport_label = sorted_backport_labels[0]
            remaining_labels = sorted_backport_labels[1:]  # Labels for older versions
            
            version = newest_backport_label.replace('backport/', '')
            backport_base_branch = newest_backport_label.replace('backport/', backport_branch)
            
            logging.info(f"Creating cascading backport for version {version} with remaining labels: {remaining_labels}")
            backport(repo, pr, version, commits, backport_base_branch, remaining_labels)
        else:
            # Single backport label - process normally
            backport_label = sorted_backport_labels[0]
            version = backport_label.replace('backport/', '')
            backport_base_branch = backport_label.replace('backport/', backport_branch)
            
            logging.info(f"Creating single backport for version {version}")
            backport(repo, pr, version, commits, backport_base_branch)


if __name__ == "__main__":
    main()
