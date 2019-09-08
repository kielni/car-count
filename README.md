# Hillbrook traffic data collection and alerting

## prerequisites

Set up a python3 3.7 virtual env:
  - install [pyenv](https://github.com/pyenv/pyenv)
  - install [pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv)

```
pyenv install 3.7.1
pyenv virtualenv 3.7.1 aws-lambda-37
pyenv activate aws-lambda-37
pip install -r requirements.txt
```

Install serverless and dependencies packaging plugin:
  - [install serverless](https://serverless.com/framework/docs/providers/aws/guide/quick-start/)
  - install plugin for packaging dependencies: `sls plugin install -n serverless-python-requirements`

## deploy

`sls deploy`
