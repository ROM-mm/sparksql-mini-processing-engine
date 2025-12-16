# Bug Fixes Summary

This document details the 3 bugs found and fixed in the codebase.

---

## Bug 1: Hardcoded Absolute Paths (Security/Portability Issue)

### Location
`pipeline.yml` - Lines 41, 51, 56, 68, 82, 102

### Description
The configuration file contained hardcoded absolute paths specific to one developer's machine:
```yaml
path: "/Users/romerito.morais/Documents/projects/develop/declarative-pipelines/lakehouse/stage/*.csv"
```

### Impact
- **Security Risk**: Exposes developer's username and directory structure
- **Portability**: Code won't work on other machines, CI/CD environments, or for other developers
- **Maintainability**: Requires manual path updates for each environment

### Root Cause
Configuration paths were copied from a development environment without being made relative to the project root.

### Fix
Replaced all absolute paths with relative paths from the project root:

**Before:**
```yaml
source:
  path: "/Users/romerito.morais/Documents/projects/develop/declarative-pipelines/lakehouse/stage/*.csv"
export:
  parquet:
    path: "/Users/romerito.morais/Documents/projects/develop/declarative-pipelines/lakehouse/silver/table_customers_final"
```

**After:**
```yaml
source:
  path: "lakehouse/stage/*.csv"
export:
  parquet:
    path: "lakehouse/silver/table_customers_final"
```

### Files Modified
- `pipeline.yml`

### Benefits
- ✅ Works on any machine without modification
- ✅ No sensitive information exposed
- ✅ CI/CD and containerization compatible
- ✅ Easier to maintain and share

---

## Bug 2: Type Annotation Incompatibility (Python Compatibility Issue)

### Location
`src/modules/sql_template_processor.py` - Line 143

### Description
The method `validate_refs()` used lowercase `tuple` type annotation:
```python
def validate_refs(...) -> tuple[bool, List[str]]:
```

This syntax only works in **Python 3.9+** and causes a `TypeError` in Python 3.7-3.8.

### Impact
- **Compatibility**: Code breaks on Python 3.7 and 3.8
- **Runtime Error**: Fails at import time with `TypeError: 'type' object is not subscriptable`
- **Project Requirements**: The project uses `pyspark==4.1.0.dev3` which may need older Python versions

### Root Cause
Python 3.9 introduced support for using built-in collection types (like `tuple`, `list`, `dict`) directly in type hints. Before Python 3.9, you needed to import `Tuple`, `List`, `Dict` from the `typing` module.

### Fix
Changed lowercase `tuple` to `Tuple` from the typing module:

**Before:**
```python
from typing import Dict, List, Optional, Set
# ...
def validate_refs(...) -> tuple[bool, List[str]]:
```

**After:**
```python
from typing import Dict, List, Optional, Set, Tuple
# ...
def validate_refs(...) -> Tuple[bool, List[str]]:
```

### Files Modified
- `src/modules/sql_template_processor.py`

### Benefits
- ✅ Compatible with Python 3.7, 3.8, 3.9, 3.10, 3.11, 3.12+
- ✅ No runtime errors on older Python versions
- ✅ Consistent with other type annotations in the file

---

## Bug 3: Missing Validation in Config Loader (Logic Error/Crash Risk)

### Location
`src/modules/config_loader.py` - Line 97 (original), now lines 86-148

### Description
The `get_datalake_zones()` method assumed that each zone dictionary in the YAML has 'name' and 'path' keys:
```python
return {zone['name']: zone['path'] for zone in zones}
```

If the YAML configuration is malformed or missing these keys, the code raises an unhelpful `KeyError`.

### Impact
- **Crash Risk**: Application crashes with cryptic `KeyError` on malformed YAML
- **Poor UX**: No helpful error message to guide fixing the configuration
- **Debugging Difficulty**: Hard to identify which zone or what field is missing

### Example of Problematic YAML
```yaml
lakehouse_zones:
  zones:
    - name: stage
      path: lakehouse/stage
    - path: lakehouse/silver  # Missing 'name' key - would cause KeyError
    - name: gold               # Missing 'path' key - would cause KeyError
```

### Root Cause
No validation of configuration structure before accessing dictionary keys.

### Fix
Added comprehensive validation with helpful error messages:

**Before:**
```python
def get_datalake_zones(self) -> Dict[str, str]:
    config = self._load_pipeline_yml()
    zones_config = config.get('lakehouse_zones', {})
    zones = zones_config.get('zones', [])
    return {zone['name']: zone['path'] for zone in zones}
```

**After:**
```python
def get_datalake_zones(self) -> Dict[str, str]:
    """
    Retorna mapeamento de zonas do datalake.
    
    Returns:
        Dicionário com nome da zona: caminho
        
    Raises:
        ValueError: Se a configuração de zonas está malformada
    """
    config = self._load_pipeline_yml()
    zones_config = config.get('lakehouse_zones', {})
    zones = zones_config.get('zones', [])
    
    # Validar que zones é uma lista
    if not isinstance(zones, list):
        raise ValueError(
            f"Configuração 'lakehouse_zones.zones' deve ser uma lista, "
            f"mas encontrado tipo: {type(zones).__name__}"
        )
    
    # Validar cada zona e construir mapeamento
    zone_mapping = {}
    for idx, zone in enumerate(zones):
        # Validar que zona é um dicionário
        if not isinstance(zone, dict):
            raise ValueError(
                f"Zona na posição {idx} deve ser um dicionário, "
                f"mas encontrado tipo: {type(zone).__name__}"
            )
        
        # Validar campos obrigatórios
        if 'name' not in zone:
            raise ValueError(
                f"Zona na posição {idx} está faltando o campo obrigatório 'name'. "
                f"Campos disponíveis: {list(zone.keys())}"
            )
        
        if 'path' not in zone:
            raise ValueError(
                f"Zona '{zone.get('name', f'posição {idx}')}' está faltando o campo obrigatório 'path'. "
                f"Campos disponíveis: {list(zone.keys())}"
            )
        
        zone_name = zone['name']
        zone_path = zone['path']
        
        # Validar que name e path são strings
        if not isinstance(zone_name, str):
            raise ValueError(
                f"Campo 'name' da zona na posição {idx} deve ser string, "
                f"mas encontrado tipo: {type(zone_name).__name__}"
            )
        
        if not isinstance(zone_path, str):
            raise ValueError(
                f"Campo 'path' da zona '{zone_name}' deve ser string, "
                f"mas encontrado tipo: {type(zone_path).__name__}"
            )
        
        zone_mapping[zone_name] = zone_path
    
    return zone_mapping
```

### Files Modified
- `src/modules/config_loader.py`

### Validation Checks Added
1. ✅ Validates that `zones` is a list
2. ✅ Validates that each zone is a dictionary
3. ✅ Validates that required keys ('name', 'path') exist
4. ✅ Validates that 'name' and 'path' values are strings
5. ✅ Provides helpful error messages with context

### Error Message Examples
```
ValueError: Zona na posição 1 está faltando o campo obrigatório 'name'. Campos disponíveis: ['path', 'description']

ValueError: Campo 'path' da zona 'gold' deve ser string, mas encontrado tipo: int

ValueError: Configuração 'lakehouse_zones.zones' deve ser uma lista, mas encontrado tipo: dict
```

### Benefits
- ✅ Prevents crashes from malformed YAML
- ✅ Clear, actionable error messages
- ✅ Easier debugging and configuration
- ✅ Better developer experience
- ✅ Follows defensive programming practices

---

## Summary

| Bug # | Type | Severity | Impact | Status |
|-------|------|----------|--------|--------|
| 1 | Security/Portability | High | Hardcoded paths break portability | ✅ Fixed |
| 2 | Compatibility | Medium | Python 3.7-3.8 compatibility broken | ✅ Fixed |
| 3 | Logic Error | Medium | Crashes on malformed config | ✅ Fixed |

## Testing Recommendations

1. **Bug 1**: Test on different machines and with CI/CD to verify paths work
2. **Bug 2**: Test with Python 3.7, 3.8, and 3.9+ to verify compatibility
3. **Bug 3**: Test with malformed YAML configs to verify error messages

## Additional Notes

All fixes follow best practices:
- No breaking changes to existing functionality
- Backward compatible
- Better error handling and user experience
- Improved code maintainability and security
