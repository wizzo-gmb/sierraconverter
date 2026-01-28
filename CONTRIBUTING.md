# Contributing to SCID Converter

Thank you for your interest in contributing to the SCID Converter project! This document provides guidelines and information for contributors.

## 🚀 Getting Started

### Prerequisites
- Python 3.7 or higher
- Git
- Basic understanding of financial tick data

### Setting Up Development Environment

1. **Fork the Repository**
   ```bash
   # Fork the repository on GitHub, then clone your fork
   git clone https://github.com/yourusername/scid-converter.git
   cd scid-converter
   ```

2. **Create Virtual Environment**
   ```bash
   python -m venv venv
   
   # On Windows
   venv\Scripts\activate
   
   # On macOS/Linux
   source venv/bin/activate
   ```

3. **Install Dependencies**
   ```bash
   # Install in development mode
   pip install -e .
   
   # Install development dependencies
   pip install -e .[dev]
   ```

4. **Verify Setup**
   ```bash
   # Run tests to ensure everything works
   python -m pytest tests/
   ```

## 📝 Development Workflow

### 1. Create a Branch
```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/your-bug-fix
```

### 2. Make Changes
- Follow the existing code style
- Add tests for new functionality
- Update documentation as needed

### 3. Run Tests
```bash
# Run all tests
python -m pytest tests/

# Run with coverage
python -m pytest --cov=scid_converter tests/

# Run specific test file
python -m pytest tests/test_converter.py
```

### 4. Code Quality
```bash
# Format code
black scid_to_nt8_converter.py tests/ examples/

# Check linting
flake8 scid_to_nt8_converter.py tests/ examples/

# Type checking (optional)
mypy scid_to_nt8_converter.py
```

### 5. Commit Changes
```bash
# Stage changes
git add .

# Commit with clear message
git commit -m "feat: add support for new symbol XYZ"

# Push to your fork
git push origin feature/your-feature-name
```

### 6. Create Pull Request
- Go to your fork on GitHub
- Click "New Pull Request"
- Fill out the PR template
- Wait for review

## 🎯 Contribution Guidelines

### Code Style
- Follow PEP 8 style guidelines
- Use meaningful variable and function names
- Add docstrings to new functions
- Keep functions focused and small

### Testing
- Write tests for new features
- Ensure all tests pass before submitting
- Aim for high code coverage
- Test edge cases and error conditions

### Documentation
- Update README.md for user-facing changes
- Add inline comments for complex logic
- Update docstrings for function changes
- Add examples for new features

### Commit Messages
Use conventional commit format:
- `feat:` for new features
- `fix:` for bug fixes
- `docs:` for documentation changes
- `style:` for formatting changes
- `refactor:` for code refactoring
- `test:` for test changes
- `chore:` for maintenance tasks

Examples:
```
feat: add support for MetaTrader 5 export format
fix: handle invalid SCID file headers gracefully
docs: update installation instructions
test: add tests for custom threshold functionality
```

## 🐛 Bug Reports

When reporting bugs, please include:

1. **Environment Information**
   - Python version
   - Operating system
   - SCID file details (size, symbol, date range)

2. **Steps to Reproduce**
   - Clear, step-by-step instructions
   - Sample SCID file (if possible)
   - Command line arguments used

3. **Expected vs Actual Behavior**
   - What you expected to happen
   - What actually happened
   - Error messages (full traceback)

4. **Additional Context**
   - Any relevant configuration
   - Related issues or discussions

## 💡 Feature Requests

When requesting features, please include:

1. **Use Case**
   - Why this feature is needed
   - What problem it solves
   - Who would benefit

2. **Requirements**
   - Detailed description of the feature
   - Expected behavior
   - Integration points

3. **Implementation Ideas** (optional)
   - How you think it could be implemented
   - Potential challenges
   - Alternative approaches

## 🔧 Development Areas

### High Priority Areas
1. **Performance Optimization**
   - Faster file processing
   - Memory usage improvements
   - Parallel processing enhancements

2. **Format Support**
   - Additional trading platforms
   - Different output formats
   - Custom format specifications

3. **Cleaning Algorithms**
   - Advanced outlier detection
   - Machine learning approaches
   - Adaptive thresholds

### Medium Priority Areas
1. **User Experience**
   - Progress bars
   - Better error messages
   - Configuration files

2. **Testing**
   - More comprehensive test coverage
   - Integration tests
   - Performance benchmarks

3. **Documentation**
   - API documentation
   - Tutorials
   - FAQ section

## 🏷️ Release Process

1. **Version Bumping**
   - Update version in setup.py
   - Update CHANGELOG.md
   - Create git tag

2. **Testing**
   - Full test suite
   - Manual testing
   - Performance benchmarks

3. **Documentation**
   - Update README
   - Review docs
   - Check examples

4. **Release**
   - Create GitHub release
   - Upload to PyPI (if applicable)
   - Announce changes

## 🤝 Code Review Process

### Reviewer Guidelines
1. **Code Quality**
   - Check for bugs and edge cases
   - Verify test coverage
   - Assess performance impact

2. **Design**
   - Evaluate architecture decisions
   - Check for maintainability
   - Consider alternatives

3. **Documentation**
   - Verify docs are updated
   - Check examples work
   - Assess clarity

### Author Guidelines
1. **Respond to Feedback**
   - Address reviewer comments
   - Explain design decisions
   - Update code as needed

2. **Be Patient**
   - Reviews take time
   - Maintain politeness
   - Help reviewers understand

## 📞 Getting Help

- **Discussions**: Use GitHub Discussions for questions
- **Issues**: Use GitHub Issues for bugs and features
- **Email**: Contact maintainers for sensitive topics

## 📄 License

By contributing to this project, you agree that your contributions will be licensed under the MIT License.

## 🙏 Recognition

Contributors will be recognized in:
- README.md contributors section
- Release notes
- Commit history

Thank you for contributing to SCID Converter! 🎉
