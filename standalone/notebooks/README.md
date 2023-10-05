# Tangara Notebooks

## Execute notebook with nbconvert

Use the terminal and execute any Jupyter Notebook by using the nbconvert tool.
This tool executes the notebook and overwrites your existing notebook with the most recent computations:

```bash
$promt> jupyter nbconvert --execute --to notebook --inplace your-notebook.ipynb
# Example:
$promt> jupyter nbconvert --execute --to notebook --inplace tangaras.ipynb
```
