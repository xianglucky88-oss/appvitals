PY=python3
run:
	PYTHONPATH=src $(PY) -m appvitals.cli
test:
	$(PY) run_tests.py
clean:
	rm -rf .pytest_cache **/__pycache__ *.db
