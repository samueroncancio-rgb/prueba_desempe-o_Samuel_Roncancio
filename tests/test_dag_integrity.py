import os
import glob
import ast
import pytest

def get_dag_files():
    dag_pattern = os.path.join(os.path.dirname(__file__), "..", "airflow", "dags", "dag_*.py")
    files = glob.glob(dag_pattern)
    return files

def test_dag_files_exist():
    files = get_dag_files()
    assert len(files) >= 3, f"Se esperaban al menos 3 DAGs, encontrados: {len(files)}"

@pytest.mark.parametrize("dag_path", get_dag_files())
def test_dag_syntax_and_ast_integrity(dag_path):
    """
    Verifica que cada archivo de DAG tenga sintaxis válida en Python,
    sin errores de compilación ni estructuras anómalas.
    """
    with open(dag_path, "r", encoding="utf-8") as f:
        source = f.read()
    
    # Parse AST
    parsed_ast = ast.parse(source, filename=dag_path)
    assert parsed_ast is not None

    # Verify presence of @dag decorator or dag definitions
    assert "dag" in source.lower()
    assert "default_args" in source or "dag_id" in source
