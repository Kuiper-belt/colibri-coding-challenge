# colibri-coding-challenge

# Setting Up a Python Virtual Environment

Follow these steps to set up a Python virtual environment for your project.

## 1. Install Python and Virtualenv

1. **Check if Python is Installed**:
   ```bash
   python --version
   ```

2. **Install Virtualenv** (if not already installed):
   ```bash
   pip install virtualenv
   ```

3. **Create the Virtual Environment**:
   Navigate to your project directory and create the virtual environment:
   ```bash
   cd <your_project_directory>
   python -m venv venv
   ```

---

## 2. Create the Project Directory

1. Create a new directory for your project (if not already created):
   ```bash
   mkdir <project_name>
   cd <project_name>
   ```

---

## 3. Create a Virtual Environment

1. Use `virtualenv` to create the virtual environment:
   ```bash
   python -m venv venv
   ```

   This will create a directory named `venv` in your project folder.

---

## 4. Activate the Virtual Environment

### Linux/macOS:
```bash
source venv/bin/activate
```

### Windows:
```bash
venv\Scripts\activate
```

You should now see `(venv)` at the beginning of your terminal prompt.

---

## 5. Install Required Libraries

1. Install the libraries needed for your project:
   ```bash
   pip install apache-airflow pyspark
   ```

2. Save the dependencies to a `requirements.txt` file:
   ```bash
   pip freeze > requirements.txt
   ```

---

## 6. Deactivate the Virtual Environment

When you are done working in the virtual environment, deactivate it:
```bash
deactivate
```

---

## 7. Reactivate the Virtual Environment

To reactivate the virtual environment later, navigate to your project directory and run the activation command again:

### Linux/macOS:
```bash
source venv/bin/activate
```

### Windows:
```bash
venv\Scripts\activate
```
