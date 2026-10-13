"""The production Gold run gates R3: the notebook passes the inventory reconciliation.

``write_gold`` accepts it as optional (tests build Gold without Silver); omitting it in the
notebook would silently drop the rule, so the wiring is a contract.
"""

import ast
from pathlib import Path

NOTEBOOK = Path("projects/enterprise-retail-lakehouse/notebooks/gold/01_build_gold.py")


def test_the_gold_notebook_passes_the_inventory_reconciliation(repo_root: Path) -> None:
    tree = ast.parse((repo_root / NOTEBOOK).read_text(encoding="utf-8"))
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "write_gold"
    ]

    assert len(calls) == 1
    assert "inventory_reconciliation" in {keyword.arg for keyword in calls[0].keywords}
