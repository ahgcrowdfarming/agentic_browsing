import yaml
import os

def test_yaml_exists_and_valid():
    yaml_path = os.path.join(
        os.path.dirname(__file__),
        'agent_tasks',
        'lidl-germany.yaml'
    )
    assert os.path.exists(yaml_path), "YAML file does not exist"
    with open(yaml_path, encoding='utf-8') as f:
        data = yaml.safe_load(f)
    assert 'name' in data
    assert 'task' in data
    assert 'judge_context' in data
    assert 'max_steps' in data