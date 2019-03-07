python_version  = 3.6.4

pyenv:
	@pyenv install -s $(python_version)
	@pyenv virtualenvs | grep " $(python_version)/envs/s3compressor " || pyenv virtualenv $(python_version) s3compressor
	@${HOME}/.pyenv/versions/$(python_version)/envs/s3compressor/bin/pip install -r requirements.txt
	@echo $(python_version)/envs/s3compressor > .python-version

pyenv-uninstall:
	@rm .python-version
	@pyenv uninstall -f s3compressor
