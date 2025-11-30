"""
Helper para criar SparkApplications de forma padronizada.
Abstrai a complexidade de configuração Kubernetes do desenvolvedor.
"""

from typing import List, Optional, Dict
import yaml
import os


class SparkJobBuilder:
    """
    Builder pattern para criar SparkApplications seguindo padrões da empresa.
    
    Exemplo de uso:
        job = SparkJobBuilder('meu-job', 'script.py')
            .with_arguments(['arg1', 'arg2'])
            .with_resources(executor_instances=3)
            .build()
    """
    
    TEMPLATE_PATH = os.path.join(
        os.path.dirname(__file__),
        '../spark-apps/templates/spark-base-template.yaml'
    )
    
    def __init__(self, job_name: str, script_file: str):
        """
        Inicializa o builder.
        
        Args:
            job_name: Nome único do job Spark
            script_file: Nome do arquivo Python em spark-scripts/
        """
        self.job_name = job_name
        self.script_file = script_file
        self.arguments = []
        self.spec = self._load_template()
    
    def _load_template(self) -> dict:
        """Carrega template base do repositório"""
        with open(self.TEMPLATE_PATH, 'r') as f:
            return yaml.safe_load(f)
    
    def with_arguments(self, args: List[str]) -> 'SparkJobBuilder':
        """
        Adiciona argumentos para o script Spark.
        
        Args:
            args: Lista de argumentos (ex: paths S3)
        """
        self.arguments = args
        return self
    
    def with_resources(self, 
                      driver_cores: int = 1,
                      driver_memory: str = "1g",
                      executor_cores: int = 1,
                      executor_memory: str = "1g", 
                      executor_instances: int = 2) -> 'SparkJobBuilder':
        """
        Configura recursos computacionais.
        
        Args:
            driver_cores: Número de cores do driver
            driver_memory: Memória do driver (ex: "2g", "512m")
            executor_cores: Número de cores por executor
            executor_memory: Memória por executor
            executor_instances: Número de executors
        """
        self.spec['spec']['driver']['cores'] = driver_cores
        self.spec['spec']['driver']['memory'] = driver_memory
        self.spec['spec']['executor']['cores'] = executor_cores
        self.spec['spec']['executor']['memory'] = executor_memory
        self.spec['spec']['executor']['instances'] = executor_instances
        return self
    
    def with_labels(self, labels: Dict[str, str]) -> 'SparkJobBuilder':
        """
        Adiciona labels customizadas aos pods.
        
        Args:
            labels: Dicionário de labels (ex: {'team': 'data', 'env': 'prod'})
        """
        self.spec['spec']['driver']['labels'].update(labels)
        self.spec['spec']['executor']['labels'].update(labels)
        return self
    
    def with_spark_conf(self, conf: Dict[str, str]) -> 'SparkJobBuilder':
        """
        Adiciona configurações Spark customizadas.
        
        Args:
            conf: Dicionário de configurações Spark
        """
        if 'sparkConf' not in self.spec['spec']:
            self.spec['spec']['sparkConf'] = {}
        self.spec['spec']['sparkConf'].update(conf)
        return self
    
    def build(self) -> dict:
        """
        Constrói o objeto SparkApplication final.
        
        Returns:
            Dicionário com a especificação completa da SparkApplication
        """
        # Substitui placeholders
        self.spec['metadata']['name'] = self.job_name
        self.spec['spec']['mainApplicationFile'] = \
            f"local:///opt/spark/git-repo/spark-scripts/{self.script_file}"
        self.spec['spec']['arguments'] = self.arguments
        
        return self.spec


def create_financial_spark_job(job_name: str, 
                               script_file: str, 
                               input_paths: List[str],
                               output_path: str,
                               executor_instances: int = 2) -> dict:
    """
    Factory function para jobs financeiros padrão.
    Configuração pré-definida para ETLs financeiros.
    
    Args:
        job_name: Nome do job
        script_file: Script Python
        input_paths: Lista de paths de entrada (S3)
        output_path: Path de saída (S3)
        executor_instances: Número de executors
    
    Returns:
        SparkApplication spec
    """
    return SparkJobBuilder(job_name, script_file) \
        .with_arguments(input_paths + [output_path]) \
        .with_resources(executor_instances=executor_instances) \
        .with_labels({
            'team': 'data-engineering',
            'domain': 'financial',
            'cost-center': 'analytics'
        }) \
        .build()