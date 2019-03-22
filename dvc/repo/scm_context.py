def scm_context(method):
    def run(repo, *args, **kw):
        try:
            result = method(repo, *args, **kw)
            repo.scm.reset_ignores()
            repo.scm.remind_to_track()
            return result
        except Exception as e:
            repo.scm.cleanup_ignores()
            raise e

    return run
