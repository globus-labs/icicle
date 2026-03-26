find . | grep -E "(/__pycache__$|\.pyc$|\.pyo$)" | xargs rm -rf
rm -rf target

cp helper_itap.py helper_nersc.py helper_hpss.py calc.py conf.py search.py my_deps
# Note: when files are unzipped, they are NOT under the my_deps/ folder
# so that we can "import ddsketch" in the code
cd my_deps
zip -r ../my_deps.zip *
cd ..

# Note: when files are unzipped, they are under the utils/ folder
# so that we can "from utils.kdalogging import get_logger" in the code
zip -r utils.zip utils

# Note: when files are unzipped, they are NOT under the resources/ folder
cd resources
zip -r ../resources.zip *
cd ..

mvn clean package
aws s3 cp target/initial-ingest-1.0.0.zip s3://search-alpha-bucket-1

# cleanup
rm -rf resources.zip my_deps/helper_itap.py my_deps/helper_nersc.py my_deps/helper_hpss.py my_deps/calc.py my_deps/conf.py my_deps/search.py my_deps.zip utils.zip dependency-reduced-pom.xml
