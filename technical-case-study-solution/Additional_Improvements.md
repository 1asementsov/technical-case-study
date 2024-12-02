## Additional Improvements

### Here I have additional notes on what can be improved further if needed 
### For example:
1. CI/CD like adding pre-commit to run pyton lint
2. How to create github action to run test on Spark in github actions container
3. Integrating Pyspark Style Guideline Checks into Our CI/CD Pipeline

### 1. Setting up Pre-commit for Python Linting

Pre-commit is a tool that helps us manage and maintain multi-language pre-commit hooks. 
These hooks can be used to automatically run tasks like linters or formatters before committing changes.

#### Step 1: Install Pre-commit
First, we'll need to install pre-commit locally in our project:

```bash
pip install pre-commit
```

#### Step 2: Add Pre-commit Configuration
Next, we create a .pre-commit-config.yaml file in the root of our project. 
This file will define the hooks that will run before each commit. 

Below is an example configuration that sets up flake8 for Python linting:

```yaml
- repo: https://github.com/pre-commit/mirrors-flake8
  rev: v5.0.4
  hooks:
    - id: flake8
      args: ['--max-line-length=88']
```

#### Step 3: Install the Pre-commit Hooks
Once the .pre-commit-config.yaml file is created, we install the hooks by running:

```bash
pre-commit install
```

This command will set up a Git hook in .git/hooks/pre-commit that automatically runs the configured checks every time we commit code.

#### Step 4: Running the Pre-commit Hooks Manually
To manually run all configured hooks on all files, we can use the following command:

```bash
pre-commit run --all-files
```

This is useful if we want to check all files in the repository before committing changes.

### 2. GitHub Actions Workflow for Spark Tests

GitHub Actions is a powerful tool for automating CI/CD workflows. We can create custom workflows to run tests on Spark in a containerized environment.

#### Step 1: Set up the Workflow File
We need to create a .github/workflows/spark-tests.yml file in our repository. This YAML file defines the workflow, specifying when to trigger the job and the steps to run tests in a Spark container.

```yaml
name: Spark Tests

on:
  push:
    branches:
      - main
  pull_request:
    branches:
      - main

jobs:
  test:
    runs-on: ubuntu-latest

    services:
      spark:
        image: bitnami/spark:3.1
        options: --health-cmd="curl --silent --fail localhost:8080" --health-timeout=30s --health-start-period=10s --health-retries=3
        ports:
          - 8080:8080

    steps:
      - name: Checkout code
        uses: actions/checkout@v2

      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.8'

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt
          pip install pytest

      - name: Run tests
        run: |
          pytest tests/
```

- **Trigger**: This workflow is triggered on pushes and pull requests to the main branch.

- **Job**: The test job runs on an ubuntu-latest container and starts a Spark service using the Bitnami Spark container image.

- **Python Setup**: The workflow sets up Python and installs project dependencies (including pytest).

- **Running Tests**: Finally, the workflow runs the tests using pytest in the tests/ directory.

#### Step 2: Push the Workflow to GitHub

Once we've created the .github/workflows/spark-tests.yml file, we commit and push it to GitHub. This will trigger the workflow on the next push or pull request to the main branch.

```bash
git add .github/workflows/spark-tests.yml
git commit -m "Add Spark test CI pipeline"
git push origin master
```

#### Step 3: Review Test Results
After pushing our changes, GitHub Actions will automatically execute the workflow. 
We can view the progress and results under the Actions tab in our GitHub repository.


## PySpark Style Guide for our CI/CD pipeline

Integrating the Palantir PySpark Style Guide into our CI/CD pipeline can enhance code quality and maintainability. This guide offers best practices for PySpark development, focusing on readability and consistency.

Key Recommendations from the PySpark Style Guide:

1. Prefer Implicit Column Selection: Use F.col('column_name') for column references to avoid issues with column names containing spaces or special characters.

```py
# Preferred
df = df.select(F.col('column_name'))
```

2. Refactor Complex Logical Operations: Break down complex conditions into named variables to improve readability.

```py
is_delivered = (F.col('prod_status') == 'Delivered')
is_active = (F.col('currentRegistration').rlike('.+')) | (F.col('originalOperator') != '')
df = df.filter(is_delivered | is_active)
```

3. Use select Statements to Specify Schema Contracts: Define the expected schema at the beginning of transformations to clarify data structure.

```py
df = df.select(
    'column1',
    'column2',
    F.col('column3').alias('new_column3')
)
```
#### Integrating Style Checks into Our CI/CD Pipeline:
To enforce these practices, we can incorporate style checks into our CI/CD pipeline using tools like pylint with custom configurations.

1. **Install pylint:**

```bash
pip install pylint
```

2. **Configure pylint with Custom Rules:** Create a .pylintrc file with configurations that align with the PySpark Style Guide.

3. **Integrate into GitHub Actions Workflow:** Add a step in our GitHub Actions workflow to run pylint checks.
```yaml
- name: Run Pylint
  run: |
    pip install pylint
    pylint --rcfile=.pylintrc path/to/our/code
```

By adopting these practices and integrating them into our CI/CD pipeline, we can ensure our PySpark codebase remains clean, consistent, and maintainable.

---