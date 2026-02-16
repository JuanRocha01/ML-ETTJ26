from __future__ import annotations

from pathlib import Path


class ProjectRootNotFoundError(RuntimeError):
    pass


def project_root(start_path: Path | None = None) -> Path:
    """
    Resolve dinamicamente o root do projeto procurando por 'pyproject.toml'.

    Estratégia:
        1. Começa do diretório atual (cwd) OU do start_path fornecido.
        2. Sobe na hierarquia de pastas.
        3. Retorna o primeiro diretório que contém 'pyproject.toml'.
        4. Se não encontrar, lança erro explícito.

    Isso é robusto para:
        - execução via CLI
        - Kedro
        - Docker
        - Jupyter
        - pytest
        - execução a partir de subpastas

    Parâmetro opcional:
        start_path: usado principalmente para testes ou cenários especiais.
    """
    current = (start_path or Path.cwd()).resolve()

    for parent in [current] + list(current.parents):
        if (parent / "pyproject.toml").exists():
            return parent

    raise ProjectRootNotFoundError(
        f"Não foi possível localizar o root do projeto "
        f"(pyproject.toml não encontrado acima de {current})."
    )
