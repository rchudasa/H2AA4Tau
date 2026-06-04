from coffea.processor import IterativeExecutor, Runner

executor = IterativeExecutor()
runner = processor.Runner(
    executor=executor,
    schema=NanoAODSchema,
    chunksize=500000,
    maxchunks=None
)

# Run the processor
result = runner(
    fileset,
    treename='Events',
    processor_instance=FourTauProcessor(year="2023")
)

# Save or inspect the output
print(result)

from coffea import util
util.save(result, f"output/histos_v1.coffea")