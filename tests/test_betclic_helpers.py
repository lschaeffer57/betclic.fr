"""Unit tests for pure helpers (no network)."""


def test_norm_key_accent(betclic):
    assert betclic._norm_key("Équipe") == "equipe"
    assert betclic._norm_key("  Mi-Temps  ") == "mi-temps"


def test_parse_over_under_line(betclic):
    assert betclic._parse_over_under_line("+ de 2,5") == ("Over", "2.5")
    assert betclic._parse_over_under_line("- de 1,5") == ("Under", "1.5")
    assert betclic._parse_over_under_line("Over 200.5") == ("Over", "200.5")
    assert betclic._parse_over_under_line("under 3.5") == ("Under", "3.5")


def test_parse_yes_no(betclic):
    assert betclic._parse_yes_no("Oui") == "Yes"
    assert betclic._parse_yes_no("Non") == "No"
    assert betclic._parse_yes_no("maybe") is None


def test_foot_period(betclic):
    assert betclic._foot_period("Résultat mi-temps") == "ht"
    assert betclic._foot_period("Vainqueur fin de match") == "ft"


def test_basket_period(betclic):
    assert betclic._basket_period("Gagnant Q1") == "q1"
    assert betclic._basket_period("Total points mi-temps") == "ht"
    assert betclic._basket_period("Match") == "ft"


def test_hockey_period(betclic):
    assert betclic._hockey_period("1ère période") == "p1"
    assert betclic._hockey_period("Temps réglementaire") == "rt"
    assert betclic._hockey_period("Prolongations") == "et"


def test_tennis_scope(betclic):
    assert betclic._tennis_scope("Vainqueur du match") == "ft"
    assert betclic._tennis_scope("1er set") == "s1"


def test_pb_string_field_from_bytes(betclic):
    msg = [(3, "B", "Total de buts".encode("utf-8"))]
    assert betclic._pb_string_field(msg, 3) == "Total de buts"


def test_grpc_frame_roundtrip_length(betclic):
    payload = b"\x08\x01"
    framed = betclic._grpc_frame(payload)
    assert framed[0] == 0
    assert len(framed) == 5 + len(payload)


def test_team_name_in_market_no_false_positive(betclic):
    """'a' must not match inside 'nombre'."""
    assert not betclic._team_name_in_market("a", "nombre total de buts")
    assert betclic._team_name_in_market("psg", "total de buts psg")


def test_spec_raw_key(betclic):
    assert betclic._spec_raw_key("football") == "football_spec_raw"
    assert betclic._spec_raw_key("ice-hockey") == "ice_hockey_spec_raw"


def test_grpc_sport_code_for_request(betclic):
    assert betclic._grpc_sport_code_for_request("ice-hockey") == "ice_hockey"
    assert betclic._grpc_sport_code_for_request("football") == "football"


def test_rows_to_output_structure_dedupes_markets(betclic):
    rows = [
        {
            "sport_code": "football",
            "title": "A - B",
            "competition": "L1",
            "starts_at": "2025-01-01T00:00:00+00:00",
            "market_type": "foot_ml_ft",
            "selection": "A",
            "odds": 2.0,
        },
        {
            "sport_code": "football",
            "title": "A - B",
            "competition": "L1",
            "starts_at": "2025-01-01T00:00:00+00:00",
            "market_type": "foot_ml_ft",
            "selection": "A",
            "odds": 2.2,
        },
    ]
    payload = betclic.rows_to_output_structure(rows)
    assert betclic.count_duplicate_lines_in_output_json(payload) == {"markets": 0, "props": 0}
    m = payload["sports"]["Football"]["competitions"]["L1"][0]["markets"]
    assert len(m) == 1
    assert m[0]["odds"] == 2.2


def test_is_nba_player_prop_market(betclic):
    assert betclic._is_nba_player_prop_market("Total points du match") is False
    assert betclic._is_nba_player_prop_market("Performance — S. Curry — Total points") is True
    assert betclic._is_nba_player_prop_market("Statistiques joueur — Points + rebonds") is True
    assert betclic._is_nba_player_prop_market("Total points", "+ de 120,5") is False
    assert betclic._is_nba_player_prop_market("Performance — X — Points", "+ de 20,5") is True
    assert betclic._is_nba_player_prop_market("Philadelphie 76ers - Oklahoma City Thunder") is False
