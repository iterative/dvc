from . import locked
from .scm_context import scm_context


@locked
@scm_context
def run(self, no_exec=False, **kwargs):
    from dvc.stage import PipelineStage, Stage

    stage_cls = PipelineStage if kwargs.get("name") else Stage
    stage = stage_cls.create(self, **kwargs)
    if stage_cls == PipelineStage:
        stage.name = kwargs["name"]

    if stage is None:
        return None

    # TODO: check if the stage with given name already exists, don't allow that
    self.check_modified_graph([stage], self.pipeline_stages)
    self.pipeline_stages.append(stage)

    if not no_exec:
        stage.run(no_commit=kwargs.get("no_commit", False))

    if stage_cls == PipelineStage:
        stage.dvcfile.dump_multistages(stage, stage.path)

    stage.dump()

    return stage
