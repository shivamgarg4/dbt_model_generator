# Dataflow Pipeline Generator Roadmap

## Current Version: v1.0

## Upcoming Features

### Release v1.1

#### 1. Pipeline Tab Enhancements
- [x] Sample file generation with new design
- [x] Custom tab naming from "Generated Mapping Sheet"
- [x] Pre-hook and Post-hook configuration support

#### 2. LND Tab Customisation Implementation
- [x] LND data loading tab
- [x] LND tab with source DP column integration
- [x] Additional column support in LND

#### 3. YAML Configuration
- [X] YAML file generation
- [X] Test configuration support

#### 5. Code Structure and Testing
- [ ] Unit test implementation
- [ ] Input validation
- [ ] Error handling
- [ ] Documentation updates

## Implementation Details

### Model Types
```python
MODEL_TYPES = [
    'LND Load',
    'DW Load',
    'Merge Macro',
    'Insert Macro',
    'Delete Insert Macro',
    'DP/BI View'
]
```

### YAML Configuration Template
```yaml
model_config:
  type: "Delete Insert Macro"
  pre_hook: "your_pre_hook_statement"
  post_hook: "your_post_hook_statement"
  delete_statement: "DELETE FROM target_table WHERE condition"
  
yaml_tests:
  source_tests:
    - test_type: "not_null"
      columns: ["col1", "col2"]
  target_tests:
    - test_type: "relationships"
      columns: ["foreign_key"]
```

## Priority and Timeline
- High Priority:
  - Model Type Updates
  - LND Tab Implementation
  - YAML Configuration

- Medium Priority:
  - Pipeline Tab Enhancements
  - Pre/Post Hook Support

- Low Priority:
  - Additional UI Improvements
  - Extended Documentation

## Notes
- All new features should include appropriate error handling
- UI changes should maintain consistent design
- Each feature requires updated documentation
- Test coverage should be maintained above 90%