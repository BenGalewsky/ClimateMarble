# ClimateMarble

Author @ Yizhe & Larry

Produce CERES, MISR, and MODIS gridded radiance and insolation based on Terra Basicfusion product on NASA s3. Results are stored at orbital-level and can be further processed to generate daily, monthly, and other temporal-average data for generating Climate Marbles.


## Docker Image
```shell script
docker build -t climatemarble .
```

```shell script
docker run --rm -it \
  --mount type=bind,source="$(pwd)",target=/code \
  --mount type=bind,source="$(pwd)"/data,target=/data \
climatemarble bash
```
