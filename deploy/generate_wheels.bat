
docker build --no-cache -t egis:build-wheels .
docker run --name=build egis:build-wheels
docker cp build:/opt/python/wheels.tar .
docker rm build
