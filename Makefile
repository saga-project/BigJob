
.PHONY: clean


clean:
	-rm -rf build/ saga.egg-info/ temp/ MANIFEST dist/ *.egg-info
	make -C docs clean
	find . -name \*.pyc -exec rm -f {} \;