"""Tests for BetclicScraper classification (spec-aligned market_type)."""

import pytest


@pytest.fixture
def scraper(betclic):
    return betclic.BetclicScraper()


class TestClassifyDetailMarket:
    def test_football_btts_yes(self, scraper):
        mt, sel = scraper._classify_detail_market(
            "football",
            "Les deux équipes marquent — mi-temps",
            "Oui",
            "PSG",
            "OM",
        )
        assert mt == "foot_btts_ht"
        assert sel == "Yes"

    def test_football_double_chance(self, scraper):
        mt, sel = scraper._classify_detail_market(
            "football",
            "Double chance — fin de match",
            "1N",
            "A",
            "B",
        )
        assert mt == "foot_dc_ft"
        assert sel == "1N"

    def test_football_total_goals(self, scraper):
        mt, sel = scraper._classify_detail_market(
            "football",
            "Nombre total de buts",
            "+ de 2,5",
            "A",
            "B",
        )
        assert mt == "foot_total_goals_ft"
        assert sel == "Over 2.5"

    def test_football_handicap_before_team_score(self, scraper):
        """Handicap on goals must not be classified as team-to-score."""
        mt, sel = scraper._classify_detail_market(
            "football",
            "Handicap sur les buts — fin de match",
            "A (-1.5)",
            "A",
            "B",
        )
        assert mt == "foot_hcp_team_a_ft"
        assert "-1.5" in sel

    def test_football_ecart_de_buts_phrase(self, scraper):
        """Betclic API uses « Écart de buts » with long selection text (field-10 payload)."""
        mt, sel = scraper._classify_detail_market(
            "football",
            "Écart de buts",
            "Paris SG gagne de 2 buts ou +",
            "Paris SG",
            "OM",
        )
        assert mt == "foot_hcp_team_a_ft"
        assert "Paris SG" in sel

    def test_football_ml_draw(self, scraper):
        mt, sel = scraper._classify_detail_market(
            "football",
            "Résultat 1X2 — mi-temps",
            "nul",
            "A",
            "B",
        )
        assert mt == "foot_ml_ht"
        assert sel == "Draw"

    def test_tennis_ml(self, scraper):
        mt, sel = scraper._classify_detail_market(
            "tennis",
            "Vainqueur du match",
            "Nadal",
            "Nadal",
            "Federer",
        )
        assert mt == "tennis_ml_ft"
        assert sel == "Nadal"

    def test_tennis_ecart_de_jeux(self, scraper):
        mt, sel = scraper._classify_detail_market(
            "tennis",
            "Écart de jeux",
            "Nadal (+2.5)",
            "Nadal",
            "Federer",
        )
        assert mt == "tennis_games_hcp_ft"
        assert "Nadal" in sel and "+2.5" in sel

    def test_tennis_ecart_de_sets(self, scraper):
        mt, sel = scraper._classify_detail_market(
            "tennis",
            "Écart de sets",
            "Federer (-1.5)",
            "Nadal",
            "Federer",
        )
        assert mt == "tennis_sets_hcp_ft"
        assert "Federer" in sel and "-1.5" in sel

    def test_basket_spread(self, scraper):
        mt, sel = scraper._classify_detail_market(
            "basketball",
            "Écart de points — mi-temps",
            "Lakers (+5.5)",
            "Lakers",
            "Celtics",
        )
        assert mt == "basket_spread_ht"
        assert "Lakers" in sel and "+5.5" in sel

    def test_hockey_total(self, scraper):
        mt, sel = scraper._classify_detail_market(
            "ice-hockey",
            "Total de buts — temps réglementaire",
            "- de 5,5",
            "H1",
            "H2",
        )
        assert mt == "hockey_total_rt"
        assert sel == "Under 5.5"

    def test_prop_nba_points(self, scraper):
        mt, sel = scraper._classify_detail_market(
            "basketball",
            "Statistiques joueur — LeBron James — Total points",
            "Over 25.5",
            "A",
            "B",
        )
        assert mt == "prop_nba_points"
        assert "Over" in sel


class TestClassifySelection:
    def test_foot_ml_ft_home(self, scraper):
        mt, sel = scraper._classify_selection("Paris SG", "Paris SG", "OM", "football")
        assert mt == "foot_ml_ft"
        assert sel == "Paris SG"

    def test_foot_draw(self, scraper):
        mt, sel = scraper._classify_selection("Nul", "H", "A", "football")
        assert mt == "foot_ml_ft"
        assert sel == "Draw"
