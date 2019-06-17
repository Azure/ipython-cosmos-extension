clean:
		find . -name *.pyc -delete

build: clean
		python setup.py build sdist

release: clean 
		twine upload --repository-url https://upload.pypi.org/legacy/ dist/*


.PHONY: build

