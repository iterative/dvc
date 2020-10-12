def scm_context(method):
    def run(repo, *args, **kw):
        try:
            result = method(repo, *args, **kw)
            repo.scm.reset_ignores()
            autostage = repo.config.get("core", {}).get("autostage", False)
            if autostage:
                repo.scm.track_changed_files()
            else:
                repo.scm.remind_to_track()
            repo.scm.reset_tracked_files()
            return result
        except Exception:
            repo.scm.cleanup_ignores()
            raise

    return run
