VERBOSE ?= 0
FAIL_FAST ?= 0

ifeq ($(VERBOSE), 1)
  PYTHON_TEST_FLAGS += -v
endif

ifeq ($(FAIL_FAST), 1)
  PYTHON_TEST_FLAGS += -f
endif

run-tests:
	python -B -m unittest discover $(PYTHON_TEST_FLAGS)
