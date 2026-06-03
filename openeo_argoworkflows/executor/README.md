Example run command

docker run -v ./output:/user_data exec openeo_executor execute --process_graph '{"id": "0D31CB857AC948944448","process_graph": {"load1": {"process_id": "load_collection","arguments": {"id": "SENTINEL2_MFCOVER","spatial_extent": {"west": 16.206111259998632,"east": 16.43866694610196,"south": 48.132786167842305,"north": 48.4089705430896},"temporal_extent": ["2022-11-01T00:00:00Z","2022-11-30T00:00:00Z"],"bands": ["mfcover"],"properties": {}}},"save2": {"process_id": "save_result","arguments": {"data": {"from_node": "load1"},"format": "GTIFF"},"result": true}},"parameters": []}' --user_profile '{"OPENEO_USER_ID":"TEST", "OPENEO_JOB_ID":"TEST_JOB","OPENEO_USER_WORKSPACE":"/user_data"}' --dask_profile '{"LOCAL": true}'

To create a charliecloud image .sqfs from the Dockerfile for the use on tb:
- create a .tar from the Dockerimage: `docker save openeo-executor:latest -o openeo-executor.tar` 
- copy the .tar to the terrabyte login node: `scp openeo-executor.tar <user_id>@login.terrabyte.lrz.de:/path/to/your/dss/folder/` 
- use the latest stable charliecloud version (0.4) there `module load charliecloud/0.40`
- convert the Dockerimage to a .sqfs: `ch-convert openeo-executor.tar openeo-executor.sqfs`
- or follow: https://docs.terrabyte.lrz.de/software/containers/charliecloud/#generate-a-charliecloud-image-from-a-dockerfile

