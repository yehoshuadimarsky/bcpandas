import json
from github import Github

print("getting auth and config")
with open("./creds.json") as file:
    auth = json.load(file)

with open("./dist.json") as file:
    config = json.load(file)
print("auth and config loaded")

print("logging into GitHub")
g = Github(auth["github_token"])
repo = g.get_repo(f"{config['GH_user']}/{config['name']}")
master_branch = repo.get_branch("master")

print("Creating release")
# https://pygithub.readthedocs.io/en/latest/github_objects/Repository.html#github.Repository.Repository.create_git_release
repo.create_git_release(
    tag=config["version"],
    name=config["GH_release_name"],
    message=config["GH_release_message"],
    draft=False,
    prerelease=False,
    target_commitish=master_branch,
)
print("All done!")
