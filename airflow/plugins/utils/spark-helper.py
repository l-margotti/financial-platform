"""
Helper para criar SparkApplications de forma padronizada.
"""

from typing import List, Optional, Dict
import yaml
import os


class SparkJobBuilder:
    """Builder para criar SparkApplications"""
    
    TEMPLATE_PATH = os.path.join(
        os.path.dirname(__file__),
        '../../dags/spark-apps/templates/spark-base-template.yaml'
    )
    
    def __init__(self, job_name: str, script_file: str):
        self.job_name = job_name
        self.script_file = script_file
        self.arguments = []
        self.spec = self._load_template()
    
    def _load_template(self) -> dict:
        """Carrega template base"""
        with open(self.TEMPLATE_PATH, 'r') as f:
            return yaml.safe_load(f)
    
    def with_arguments(self, args: List[str]) -> 'SparkJobBuilder':
        """Adiciona argumentos"""
        self.arguments = args
        return self
    
    def with_resources(self, 
                      driver_cores: int = 1,
                      driver_memory: str = "1g",
                      executor_cores: int = 1,
                      executor_memory: str = "1g", 
                      executor_instances: int = 2) -> 'SparkJobBuilder':
        """Configura recursos"""
        self.spec['spec']['driver']['cores'] = driver_cores
        self.spec['spec']['driver']['memory'] = driver_memory
        self.spec['spec']['executor']['cores'] = executor_cores
        self.spec['spec']['executor']['memory'] = executor_memory
        self.spec['spec']['executor']['instances'] = executor_instances
        return self
    
    def with_labels(self, labels: Dict[str, str]) -> 'SparkJobBuilder':
        """Adiciona labels"""
        self.spec['spec']['driver']['labels'].update(labels)
        self.spec['spec']['executor']['labels'].update(labels)
        return self
    
    def build(self) -> dict:
        """Constrói SparkApplication final"""
        self.spec['metadata']['name'] = self.job_name
        self.spec['spec']['mainApplicationFile'] = \
            f"local:///opt/spark/git-repo/spark-scripts/{self.script_file}"
        self.spec['spec']['arguments'] = self.arguments
        return self.spec