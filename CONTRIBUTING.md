# Contributing to Routine Engine

Thank you for your interest in contributing to Routine Engine! We welcome contributions from the community.

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/routine-engine.git`
3. Create a virtual environment: `python -m venv venv`
4. Activate it: `source venv/bin/activate` (or `venv\Scripts\activate` on Windows)
5. Install dependencies: `pip install -e ".[dev]"`

## Development Workflow

1. Create a feature branch: `git checkout -b feature/your-feature-name`
2. Make your changes
3. Run tests: `pytest tests/`
4. Run linting: `black . && flake8 . && mypy .`
5. Commit with clear messages: `git commit -m "Add feature: description"`
6. Push to your fork: `git push origin feature/your-feature-name`
7. Create a Pull Request

## Code Style

- Use Black for formatting: `black routine_engine/`
- Follow PEP 8 guidelines
- Use type hints for all functions
- Write docstrings for all public functions

## Testing

- Write tests for new features
- Ensure all tests pass: `pytest tests/`
- Aim for >80% code coverage: `pytest --cov=routine_engine tests/`

## Reporting Issues

- Use the issue tracker on GitHub
- Provide a clear description of the problem
- Include steps to reproduce
- Include your Python version and OS

## Pull Request Guidelines

- Keep PRs focused on a single feature or fix
- Include tests for your changes
- Update documentation as needed
- Reference related issues in the PR description

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

## Questions?

Feel free to open an issue or reach out to the maintainers.

Happy coding! 🚀
