"""HTML documentation generator for streamt."""

from __future__ import annotations

import json
from pathlib import Path

from streamt.core.models import StreamtProject
from streamt.core.dag import DAG


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{project_name} - streamt docs</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #0f0f0f;
            color: #e0e0e0;
            line-height: 1.6;
        }}

        .container {{
            max-width: 1400px;
            margin: 0 auto;
            padding: 2rem;
        }}

        header {{
            border-bottom: 1px solid #333;
            padding-bottom: 1rem;
            margin-bottom: 2rem;
        }}

        h1 {{
            font-size: 2rem;
            font-weight: 600;
            color: #fff;
        }}

        h2 {{
            font-size: 1.5rem;
            font-weight: 500;
            color: #fff;
            margin-top: 2rem;
            margin-bottom: 1rem;
        }}

        h3 {{
            font-size: 1.1rem;
            font-weight: 500;
            color: #aaa;
            margin-top: 1.5rem;
            margin-bottom: 0.5rem;
        }}

        .subtitle {{
            color: #888;
            font-size: 0.9rem;
        }}

        .stats {{
            display: flex;
            gap: 2rem;
            margin: 1.5rem 0;
        }}

        .stat {{
            background: #1a1a1a;
            padding: 1rem 1.5rem;
            border-radius: 8px;
            border: 1px solid #333;
        }}

        .stat-value {{
            font-size: 2rem;
            font-weight: 600;
            color: #3b82f6;
        }}

        .stat-label {{
            color: #888;
            font-size: 0.85rem;
        }}

        .dag-container {{
            background: #1a1a1a;
            border: 1px solid #333;
            border-radius: 8px;
            padding: 2rem;
            margin: 1rem 0;
            overflow-x: auto;
        }}

        #dag {{
            width: 100%;
            height: 400px;
        }}

        .card-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(350px, 1fr));
            gap: 1rem;
            margin: 1rem 0;
        }}

        .card {{
            background: #1a1a1a;
            border: 1px solid #333;
            border-radius: 8px;
            padding: 1.25rem;
            transition: border-color 0.2s;
        }}

        .card:hover {{
            border-color: #3b82f6;
        }}

        .card-title {{
            font-weight: 500;
            color: #fff;
            margin-bottom: 0.5rem;
        }}

        .card-type {{
            display: inline-block;
            font-size: 0.75rem;
            padding: 0.25rem 0.5rem;
            border-radius: 4px;
            background: #333;
            color: #888;
            margin-bottom: 0.5rem;
        }}

        .card-type.source {{
            background: #1e3a5f;
            color: #60a5fa;
        }}

        .card-type.model {{
            background: #1e4620;
            color: #4ade80;
        }}

        .card-type.test {{
            background: #4a3520;
            color: #fbbf24;
        }}

        .card-type.exposure {{
            background: #3b1e4a;
            color: #c084fc;
        }}

        .card-description {{
            color: #888;
            font-size: 0.9rem;
            margin-top: 0.5rem;
        }}

        .card-meta {{
            display: flex;
            gap: 1rem;
            margin-top: 0.75rem;
            font-size: 0.8rem;
            color: #666;
        }}

        .tag {{
            display: inline-block;
            font-size: 0.7rem;
            padding: 0.15rem 0.4rem;
            border-radius: 3px;
            background: #333;
            color: #888;
            margin-right: 0.25rem;
        }}

        pre {{
            background: #111;
            padding: 1rem;
            border-radius: 6px;
            overflow-x: auto;
            font-size: 0.85rem;
            margin: 0.5rem 0;
        }}

        code {{
            font-family: 'SF Mono', Menlo, Monaco, monospace;
        }}

        .tabs {{
            display: flex;
            gap: 0;
            border-bottom: 1px solid #333;
            margin-bottom: 1rem;
        }}

        .tab {{
            padding: 0.75rem 1.5rem;
            cursor: pointer;
            color: #888;
            border-bottom: 2px solid transparent;
            transition: all 0.2s;
        }}

        .tab:hover {{
            color: #fff;
        }}

        .tab.active {{
            color: #3b82f6;
            border-bottom-color: #3b82f6;
        }}

        .tab-content {{
            display: none;
        }}

        .tab-content.active {{
            display: block;
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>{project_name}</h1>
            <p class="subtitle">streamt documentation</p>
        </header>

        <div class="stats">
            <div class="stat">
                <div class="stat-value">{source_count}</div>
                <div class="stat-label">Sources</div>
            </div>
            <div class="stat">
                <div class="stat-value">{model_count}</div>
                <div class="stat-label">Models</div>
            </div>
            <div class="stat">
                <div class="stat-value">{test_count}</div>
                <div class="stat-label">Tests</div>
            </div>
            <div class="stat">
                <div class="stat-value">{exposure_count}</div>
                <div class="stat-label">Exposures</div>
            </div>
        </div>

        <div class="tabs">
            <div class="tab active" onclick="showTab('dag')">DAG</div>
            <div class="tab" onclick="showTab('sources')">Sources</div>
            <div class="tab" onclick="showTab('models')">Models</div>
            <div class="tab" onclick="showTab('tests')">Tests</div>
            <div class="tab" onclick="showTab('exposures')">Exposures</div>
        </div>

        <div id="dag" class="tab-content active">
            <h2>Data Lineage</h2>
            <div class="dag-container">
                <pre>{dag_ascii}</pre>
            </div>
        </div>

        <div id="sources" class="tab-content">
            <h2>Sources</h2>
            <div class="card-grid">
                {sources_html}
            </div>
        </div>

        <div id="models" class="tab-content">
            <h2>Models</h2>
            <div class="card-grid">
                {models_html}
            </div>
        </div>

        <div id="tests" class="tab-content">
            <h2>Tests</h2>
            <div class="card-grid">
                {tests_html}
            </div>
        </div>

        <div id="exposures" class="tab-content">
            <h2>Exposures</h2>
            <div class="card-grid">
                {exposures_html}
            </div>
        </div>
    </div>

    <script>
        function showTab(tabId) {{
            document.querySelectorAll('.tab').forEach(tab => tab.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(content => content.classList.remove('active'));

            document.querySelector(`.tab[onclick="showTab('${{tabId}}')"]`).classList.add('active');
            document.getElementById(tabId).classList.add('active');
        }}
    </script>
</body>
</html>
"""


def generate_docs(project: StreamtProject, dag: DAG, output_path: Path) -> None:
    """Generate HTML documentation."""
    output_path.mkdir(parents=True, exist_ok=True)

    # Generate sources HTML
    sources_html = ""
    for source in project.sources:
        tags_html = "".join(f'<span class="tag">{tag}</span>' for tag in source.tags)
        sources_html += f"""
        <div class="card">
            <span class="card-type source">source</span>
            <h3 class="card-title">{source.name}</h3>
            <p class="card-description">{source.description or 'No description'}</p>
            <div class="card-meta">
                <span>Topic: {source.topic}</span>
                {f'<span>Owner: {source.owner}</span>' if source.owner else ''}
            </div>
            {f'<div style="margin-top: 0.5rem">{tags_html}</div>' if tags_html else ''}
        </div>
        """

    # Generate models HTML
    models_html = ""
    for model in project.models:
        tags_html = "".join(f'<span class="tag">{tag}</span>' for tag in model.tags)
        models_html += f"""
        <div class="card">
            <span class="card-type model">{model.materialized.value}</span>
            <h3 class="card-title">{model.name}</h3>
            <p class="card-description">{model.description or 'No description'}</p>
            <div class="card-meta">
                {f'<span>Owner: {model.owner}</span>' if model.owner else ''}
                <span>Access: {model.access.value}</span>
            </div>
            {f'<div style="margin-top: 0.5rem">{tags_html}</div>' if tags_html else ''}
        </div>
        """

    # Generate tests HTML
    tests_html = ""
    for test in project.tests:
        tests_html += f"""
        <div class="card">
            <span class="card-type test">{test.type.value}</span>
            <h3 class="card-title">{test.name}</h3>
            <div class="card-meta">
                <span>Model: {test.model}</span>
                <span>Assertions: {len(test.assertions)}</span>
            </div>
        </div>
        """

    # Generate exposures HTML
    exposures_html = ""
    for exposure in project.exposures:
        exposures_html += f"""
        <div class="card">
            <span class="card-type exposure">{exposure.type.value}</span>
            <h3 class="card-title">{exposure.name}</h3>
            <p class="card-description">{exposure.description or 'No description'}</p>
            <div class="card-meta">
                {f'<span>Owner: {exposure.owner}</span>' if exposure.owner else ''}
                {f'<span>Role: {exposure.role.value}</span>' if exposure.role else ''}
            </div>
        </div>
        """

    # Render HTML
    html = HTML_TEMPLATE.format(
        project_name=project.project.name,
        source_count=len(project.sources),
        model_count=len(project.models),
        test_count=len(project.tests),
        exposure_count=len(project.exposures),
        dag_ascii=dag.render_ascii(),
        sources_html=sources_html or '<p style="color: #666">No sources defined</p>',
        models_html=models_html or '<p style="color: #666">No models defined</p>',
        tests_html=tests_html or '<p style="color: #666">No tests defined</p>',
        exposures_html=exposures_html or '<p style="color: #666">No exposures defined</p>',
    )

    # Write HTML file
    index_path = output_path / "index.html"
    with open(index_path, "w") as f:
        f.write(html)

    # Write manifest JSON
    manifest_path = output_path / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(
            {
                "project": project.project.name,
                "sources": [s.model_dump() for s in project.sources],
                "models": [m.model_dump() for m in project.models],
                "tests": [t.model_dump() for t in project.tests],
                "exposures": [e.model_dump() for e in project.exposures],
                "dag": dag.to_dict(),
            },
            f,
            indent=2,
            default=str,
        )
