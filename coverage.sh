pip3 install coverage
coverage run --source=ditto -m unittest discover -s tests -v
coverage report -m
#upload to codecov.io
bash <(curl -s https://codecov.io/bash)