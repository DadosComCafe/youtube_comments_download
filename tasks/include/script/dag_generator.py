from jinja2 import Environment, FileSystemLoader
import yaml
import os


def generate_dags():
    base_dir = os.path.dirname(os.path.abspath(f"{__file__}/../"))
    env = Environment(loader=FileSystemLoader(base_dir))
    template = env.get_template("templates/dag_template.jinja2")

    for file in os.listdir(f"{base_dir}/inputs"):
        print(file)
        if file.endswith(".yaml"):
            with open(f"{base_dir}/inputs/{file}", "r") as input:
                inputs = yaml.safe_load(input)
                with open(f"dags/youtube_{inputs['video_id']}.py", "w") as file2:
                    file2.write(template.render(inputs))
