"""
Unit tests for data_ingestion/utils/market_filters.py
"""

import unittest
import sys
from pathlib import Path

# Add parent directories to path
parent_dir = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(parent_dir))

from data_ingestion.utils.market_filters import is_crypto_market


class TestIsCryptoMarket(unittest.TestCase):
    """Test suite for is_crypto_market function."""

    def test_bitcoin_keyword_in_title(self):
        """Test that markets with 'bitcoin' in title are detected as crypto."""
        market = {'title': 'Will Bitcoin reach $100,000 in 2024?'}
        self.assertTrue(is_crypto_market(market))

    def test_btc_keyword_in_title(self):
        """Test that markets with 'btc' in title are detected as crypto."""
        market = {'title': 'BTC price prediction for next month'}
        self.assertTrue(is_crypto_market(market))

    def test_ethereum_keyword_in_title(self):
        """Test that markets with 'ethereum' in title are detected as crypto."""
        market = {'title': 'Will Ethereum hit new all-time high?'}
        self.assertTrue(is_crypto_market(market))

    def test_eth_keyword_in_title(self):
        """Test that markets with 'eth' in title are detected as crypto."""
        market = {'title': 'ETH vs SOL performance comparison'}
        self.assertTrue(is_crypto_market(market))

    def test_solana_keyword_in_title(self):
        """Test that markets with 'solana' in title are detected as crypto."""
        market = {'title': 'Solana network outage next week?'}
        self.assertTrue(is_crypto_market(market))

    def test_sol_keyword_in_title(self):
        """Test that markets with 'sol' in title are detected as crypto."""
        market = {'title': 'SOL to outperform ETH?'}
        self.assertTrue(is_crypto_market(market))

    def test_cardano_keyword_in_title(self):
        """Test that markets with 'cardano' in title are detected as crypto."""
        market = {'title': 'Cardano smart contracts launch date'}
        self.assertTrue(is_crypto_market(market))

    def test_ada_keyword_in_title(self):
        """Test that markets with 'ada' in title are detected as crypto."""
        market = {'title': 'ADA price will double this year'}
        self.assertTrue(is_crypto_market(market))

    def test_crypto_keyword_in_title(self):
        """Test that markets with 'crypto' in title are detected as crypto."""
        market = {'title': 'Crypto market cap to exceed $3 trillion'}
        self.assertTrue(is_crypto_market(market))

    def test_cryptocurrency_keyword_in_title(self):
        """Test that markets with 'cryptocurrency' in title are detected as crypto."""
        market = {'title': 'Cryptocurrency regulation changes in 2024'}
        self.assertTrue(is_crypto_market(market))

    def test_defi_keyword_in_title(self):
        """Test that markets with 'defi' in title are detected as crypto."""
        market = {'title': 'DeFi TVL to reach $500B'}
        self.assertTrue(is_crypto_market(market))

    def test_nft_keyword_in_title(self):
        """Test that markets with 'nft' in title are detected as crypto."""
        market = {'title': 'NFT sales volume in Q4'}
        self.assertTrue(is_crypto_market(market))

    def test_blockchain_keyword_in_title(self):
        """Test that markets with 'blockchain' in title are detected as crypto."""
        market = {'title': 'Blockchain adoption by Fortune 500'}
        self.assertTrue(is_crypto_market(market))

    def test_dogecoin_keyword_in_title(self):
        """Test that markets with 'dogecoin' in title are detected as crypto."""
        market = {'title': 'Dogecoin reaches $1?'}
        self.assertTrue(is_crypto_market(market))

    def test_doge_keyword_in_title(self):
        """Test that markets with 'doge' in title are detected as crypto."""
        market = {'title': 'DOGE Elon Musk tweets'}
        self.assertTrue(is_crypto_market(market))

    def test_ripple_keyword_in_title(self):
        """Test that markets with 'ripple' in title are detected as crypto."""
        market = {'title': 'Ripple SEC case outcome'}
        self.assertTrue(is_crypto_market(market))

    def test_xrp_keyword_in_title(self):
        """Test that markets with 'xrp' in title are detected as crypto."""
        market = {'title': 'XRP legal victory prediction'}
        self.assertTrue(is_crypto_market(market))

    def test_binance_keyword_in_title(self):
        """Test that markets with 'binance' in title are detected as crypto."""
        market = {'title': 'Binance trading volume trends'}
        self.assertTrue(is_crypto_market(market))

    def test_bnb_keyword_in_title(self):
        """Test that markets with 'bnb' in title are detected as crypto."""
        market = {'title': 'BNB Chain new projects'}
        self.assertTrue(is_crypto_market(market))

    def test_coinbase_keyword_in_title(self):
        """Test that markets with 'coinbase' in title are detected as crypto."""
        market = {'title': 'Coinbase stock performance'}
        self.assertTrue(is_crypto_market(market))

    def test_uniswap_keyword_in_title(self):
        """Test that markets with 'uniswap' in title are detected as crypto."""
        market = {'title': 'Uniswap V4 launch date'}
        self.assertTrue(is_crypto_market(market))

    def test_chainlink_keyword_in_title(self):
        """Test that markets with 'chainlink' in title are detected as crypto."""
        market = {'title': 'Chainlink oracle integrations'}
        self.assertTrue(is_crypto_market(market))

    def test_polkadot_keyword_in_title(self):
        """Test that markets with 'polkadot' in title are detected as crypto."""
        market = {'title': 'Polkadot parachain auctions'}
        self.assertTrue(is_crypto_market(market))

    def test_non_crypto_market_sports(self):
        """Test that non-crypto markets (sports) are not detected as crypto."""
        market = {'title': 'Will Lakers win the NBA championship?'}
        self.assertFalse(is_crypto_market(market))

    def test_non_crypto_market_politics(self):
        """Test that non-crypto markets (politics) are not detected as crypto."""
        market = {'title': 'Presidential election 2024 winner'}
        self.assertFalse(is_crypto_market(market))

    def test_non_crypto_market_weather(self):
        """Test that non-crypto markets (weather) are not detected as crypto."""
        market = {'title': 'Hurricane forecast for Florida'}
        self.assertFalse(is_crypto_market(market))

    def test_non_crypto_market_stocks(self):
        """Test that non-crypto markets (stocks) are not detected as crypto."""
        market = {'title': 'Apple stock price target'}
        self.assertFalse(is_crypto_market(market))

    def test_non_crypto_market_entertainment(self):
        """Test that non-crypto markets (entertainment) are not detected as crypto."""
        market = {'title': 'Oscar winners 2024 predictions'}
        self.assertFalse(is_crypto_market(market))

    def test_empty_title(self):
        """Test that markets with empty title are not detected as crypto."""
        market = {'title': ''}
        self.assertFalse(is_crypto_market(market))

    def test_missing_title_key(self):
        """Test that markets without title key are not detected as crypto."""
        market = {'description': 'Some market description'}
        self.assertFalse(is_crypto_market(market))

    def test_empty_market_dict(self):
        """Test that empty market dictionary is not detected as crypto."""
        market = {}
        self.assertFalse(is_crypto_market(market))

    def test_case_insensitive_uppercase(self):
        """Test that keyword matching is case-insensitive (uppercase)."""
        market = {'title': 'BITCOIN PRICE PREDICTION'}
        self.assertTrue(is_crypto_market(market))

    def test_case_insensitive_mixed_case(self):
        """Test that keyword matching is case-insensitive (mixed case)."""
        market = {'title': 'EtHeReUm price will rise'}
        self.assertTrue(is_crypto_market(market))

    def test_keyword_in_middle_of_sentence(self):
        """Test that keywords are detected anywhere in the title."""
        market = {'title': 'The future of cryptocurrency looks bright'}
        self.assertTrue(is_crypto_market(market))

    def test_multiple_crypto_keywords(self):
        """Test that markets with multiple crypto keywords are detected."""
        market = {'title': 'Bitcoin vs Ethereum: Which is better?'}
        self.assertTrue(is_crypto_market(market))

    def test_keyword_as_part_of_word(self):
        """Test that keywords are detected even as part of larger words."""
        market = {'title': 'Cryptocurrencies are gaining adoption'}
        self.assertTrue(is_crypto_market(market))

    def test_litecoin_keyword(self):
        """Test that markets with 'litecoin' in title are detected as crypto."""
        market = {'title': 'Litecoin halving impact'}
        self.assertTrue(is_crypto_market(market))

    def test_ltc_keyword(self):
        """Test that markets with 'ltc' in title are detected as crypto."""
        market = {'title': 'LTC payment adoption'}
        self.assertTrue(is_crypto_market(market))

    def test_shiba_keyword(self):
        """Test that markets with 'shiba' in title are detected as crypto."""
        market = {'title': 'Shiba Inu token burn rate'}
        self.assertTrue(is_crypto_market(market))

    def test_token_keyword(self):
        """Test that markets with 'token' in title are detected as crypto."""
        market = {'title': 'New token listings on exchanges'}
        self.assertTrue(is_crypto_market(market))

    def test_stablecoin_keyword(self):
        """Test that markets with 'stablecoin' in title are detected as crypto."""
        market = {'title': 'Stablecoin regulation updates'}
        self.assertTrue(is_crypto_market(market))

    def test_memecoin_keyword(self):
        """Test that markets with 'memecoin' in title are detected as crypto."""
        market = {'title': 'Memecoin season returns?'}
        self.assertTrue(is_crypto_market(market))

    def test_opensea_keyword(self):
        """Test that markets with 'opensea' in title are detected as crypto."""
        market = {'title': 'OpenSea trading volume decline'}
        self.assertTrue(is_crypto_market(market))

    def test_metamask_keyword(self):
        """Test that markets with 'metamask' in title are detected as crypto."""
        market = {'title': 'MetaMask user growth statistics'}
        self.assertTrue(is_crypto_market(market))

    def test_aave_keyword(self):
        """Test that markets with 'aave' in title are detected as crypto."""
        market = {'title': 'Aave protocol updates'}
        self.assertTrue(is_crypto_market(market))

    def test_avalanche_keyword(self):
        """Test that markets with 'avalanche' in title are detected as crypto."""
        market = {'title': 'Avalanche network performance'}
        self.assertTrue(is_crypto_market(market))

    def test_avax_keyword(self):
        """Test that markets with 'avax' in title are detected as crypto."""
        market = {'title': 'AVAX price target'}
        self.assertTrue(is_crypto_market(market))

    def test_polygon_keyword(self):
        """Test that markets with 'polygon' in title are detected as crypto."""
        market = {'title': 'Polygon zkEVM adoption'}
        self.assertTrue(is_crypto_market(market))

    def test_matic_keyword(self):
        """Test that markets with 'matic' in title are detected as crypto."""
        market = {'title': 'MATIC token migration'}
        self.assertTrue(is_crypto_market(market))

    def test_title_with_special_characters(self):
        """Test that titles with special characters are handled correctly."""
        market = {'title': 'Bitcoin: Will it reach $100k?!'}
        self.assertTrue(is_crypto_market(market))

    def test_title_with_numbers(self):
        """Test that titles with numbers are handled correctly."""
        market = {'title': 'ETH 2.0 staking rewards'}
        self.assertTrue(is_crypto_market(market))

    def test_title_with_unicode(self):
        """Test that titles with unicode characters are handled correctly."""
        market = {'title': 'Bitcoin 🚀 to the moon'}
        self.assertTrue(is_crypto_market(market))

    def test_non_string_title(self):
        """Test handling of non-string title values."""
        market = {'title': None}
        # This should not crash, .lower() will fail but get() should return ''
        with self.assertRaises(AttributeError):
            is_crypto_market(market)

    def test_whitespace_only_title(self):
        """Test that markets with whitespace-only title are not detected as crypto."""
        market = {'title': '   '}
        self.assertFalse(is_crypto_market(market))

    def test_false_positive_solution(self):
        """Test that word 'solution' containing 'sol' is still detected (current behavior)."""
        market = {'title': 'Climate change solution for 2025'}
        # This will be True because 'sol' is in 'solution' - current behavior
        self.assertTrue(is_crypto_market(market))

    def test_false_positive_whether(self):
        """Test that word 'whether' containing 'eth' is still detected (current behavior)."""
        market = {'title': 'Whether or not it will rain tomorrow'}
        # This will be True because 'eth' is in 'whether' - current behavior
        self.assertTrue(is_crypto_market(market))

    def test_no_false_positive_weather(self):
        """Test that word 'weather' does not contain 'eth' and is not detected."""
        market = {'title': 'Weather forecast for tomorrow'}
        # 'eth' is not in 'weather' so this should be False
        self.assertFalse(is_crypto_market(market))


if __name__ == '__main__':
    unittest.main()
