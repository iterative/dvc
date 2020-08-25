from dvc.utils.serialize._yaml import load_yaml, dumps_yaml, dump_yaml
import click

@click.command("dvc-render")
@click.option("--stage", "-s", type=str, default=None)
def dvc_render(stage):

    path = 'dvc.yaml'

    data = load_yaml(path)

    if stage is None:
        yml_str = dumps_yaml(data)
    else:
        yml_str = dumps_yaml({stage: data['stages'][stage]})

    print(yml_str)


def main():
    dvc_render()

if __name__ == '__main__':
    main()

