"""Configuration GX Core 1.x persistante et Data Docs."""
from pathlib import Path
import great_expectations as gx

def run_suite(df,name,expectations,root='gx'):
    context=gx.get_context(mode='file',project_root_dir=str(Path(root).resolve().parent))
    source=context.data_sources.add_or_update_pandas(name=f'{name}_pandas')
    asset=source.add_dataframe_asset(name=f'{name}_data')
    batch=asset.add_batch_definition_whole_dataframe('all_rows')
    suite=gx.ExpectationSuite(name=f'{name}_quality_suite',expectations=expectations)
    suite=context.suites.add_or_update(suite)
    definition=context.validation_definitions.add_or_update(gx.ValidationDefinition(name=f'{name}_validation',data=batch,suite=suite))
    checkpoint=context.checkpoints.add_or_update(gx.Checkpoint(name=f'{name}_checkpoint',validation_definitions=[definition],actions=[gx.checkpoint.UpdateDataDocsAction(name='update_data_docs')]))
    result=checkpoint.run(batch_parameters={'dataframe':df})
    context.build_data_docs()
    return context,result

def print_results(result):
    for validation in result.run_results.values():
        for row in validation.results:
            print('OK' if row.success else 'ECHEC',row.expectation_config.type,row.expectation_config.kwargs.get('column','table'),row.result.get('unexpected_count',''))
        print(validation.statistics)
