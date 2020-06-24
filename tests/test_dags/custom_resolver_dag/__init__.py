"""
r(AirflowDagTransformer):
    def __init__(self, src_dag: DAG, transformer_defaults: TransformerDefaultsConf = None):
        transformers = {
            EmrJobFlowSensor: EmrJobFlowSensorTransformer,
            EmrAddStepsOperator: EmrAddStepsOperatorTransformer,
            EmrStepSensor: EmrStepSensorTransformer,
            EmrTerminateJobFlowOperator: EmrTerminateJobFlowOperatorTransformer,
            S3KeySensor: None
        }

        transformer_resolvers = [
            ClassTransformerResolver(self.transformers),
            AncestralClassTransformerResolver(self.transformers),
            PythonCallTransformerResolver(
                callable_transformers={
                    check_for_existing_emr_cluster: CopyTransformer
                }
            )]

        super().__init__(src_dag,
                         transformer_defaults=transformer_defaults,
                         transformers=transformers,
                         transformer_resolvers=transformer_resolvers)
"""