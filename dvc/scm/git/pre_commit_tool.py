def pre_commit_tool_conf(pre_commit_path, push_path, post_checkout_path):
    return {
        "repos": [
            {
                "repo": "local",
                "hooks": [
                    {
                        "id": "dvc-pre-commit",
                        "name": "DVC Pre Commit",
                        "entry": pre_commit_path,
                        "language": "script",
                        "stages": ["commit"],
                    },
                    {
                        "id": "dvc-pre-push",
                        "name": "DVC Pre Push",
                        "entry": push_path,
                        "language": "script",
                        "stages": ["push"],
                    },
                    {
                        "id": "dvc-post-checkout",
                        "name": "DVC Post Checkout",
                        "entry": post_checkout_path,
                        "language": "script",
                        "stages": ["post-checkout"],
                    },
                ],
            }
        ]
    }


def merge_pre_commit_tool_confs(existing_conf, conf):
    if not existing_conf or "repos" not in existing_conf:
        return conf

    existing_conf["repos"].append(conf["repos"][0])
    return existing_conf
