from airflow.include.quality.self_healing_validator import SelfHealingValidator

def test_auto_healing_inverted_coordinates():
    """
    Inyecta un sismo con latitud y longitud invertidas (|lat| > 90, |lon| <= 90).
    Verifica que el motor Self-Healing detecta la anomalía, intercambia los ejes
    y marca el registro con is_auto_healed = True sin enviarlo a cuarentena.
    """
    corrupt_feature = {
        "type": "Feature",
        "id": "us_inverted_coords",
        "properties": {
            "mag": 4.2,
            "place": "Off Coast of Northern California",
            "time": 1700000000000
        },
        "geometry": {
            "type": "Point",
            # Lon = 40.5 (plausible lat), Lat = -124.2 (plausible lon) -> Invertidos
            "coordinates": [40.5, -124.2, 10.0]
        }
    }

    is_valid, record, _rejection_reason = SelfHealingValidator.validate_and_heal_event(corrupt_feature)
    assert is_valid is True
    assert record is not None
    assert record["is_auto_healed"] is True
    assert "SWAPPED_INVERTED_COORDINATES" in record["healing_action"]
    # Corregido: latitud debe ser 40.5 y longitud -124.2
    assert record["latitude"] == 40.5
    assert record["longitude"] == -124.2

def test_quarantine_irrecoverable_record():
    """
    Inyecta un registro sin ID o con coordenadas completamente absurdas.
    Verifica que es rechazado con motivo explícito para cuarentena.
    """
    invalid_feature = {
        "type": "Feature",
        "id": None, # Missing ID
        "properties": {"mag": 3.0, "place": "Somewhere"},
        "geometry": {"type": "Point", "coordinates": [0, 0]}
    }
    is_valid, record, rejection_reason = SelfHealingValidator.validate_and_heal_event(invalid_feature)
    assert is_valid is False
    assert record is None
    assert "id" in rejection_reason.lower()

def test_batch_quality_metrics_and_score():
    """
    Evalúa un lote mixto (válidos, auto-curados y rechazados)
    y verifica que el cálculo del quality_score sea exacto.
    """
    batch = [
        # 1. Válido
        {"id": "ev1", "properties": {"mag": 2.5, "place": "Alaska"}, "geometry": {"coordinates": [-150.0, 60.0, 5.0]}},
        # 2. Auto-curado (lat/lon invertidos)
        {"id": "ev2", "properties": {"mag": 3.1, "place": "Chile"}, "geometry": {"coordinates": [-30.0, -70.0, 15.0]}},
        # 3. Inválido (coordenadas faltantes)
        {"id": "ev3", "properties": {"mag": 1.0, "place": "Broken"}, "geometry": {"coordinates": []}}
    ]

    result = SelfHealingValidator.process_batch(batch, dag_id="test_dag")
    assert len(result["valid_records"]) == 2
    assert len(result["quarantine_records"]) == 1
    assert result["metrics"]["total_records"] == 3
    assert result["metrics"]["passed_records"] == 2
    assert result["metrics"]["quarantined_records"] == 1
    # 2 de 3 pasaron = 66.67%
    assert result["metrics"]["quality_score"] == 66.67
