from io import BytesIO
import math
import os
from pathlib import Path
import unittest
import click
from click.testing import CliRunner

import pandas as pd
import numpy as np
from scipy.stats import norm, kstest
from scipy.stats.contingency import expected_freq
import seaborn as sns

# from matplotlib_terminal import plt
import matplotlib.pyplot as plt

from sklearn.preprocessing import LabelEncoder

from statsmodels.stats.proportion import proportions_chisquare, proportions_ztest

import warnings
warnings.filterwarnings("ignore", category=UserWarning)

import sys
directory = os.path.abspath(__file__)
sys.path.append(os.path.join(Path(directory).parent.parent.parent.parent, "rsfb"))

@click.group
def cli():
    pass

from utils import load_config
from query import exec_query

WORKDIR = Path(__file__).parent
CONFIG = load_config("experiments/bsbm/config.yaml")["generation"]
SPARQL_ENDPOINT = CONFIG["sparql"]["endpoint"]
STATS_SIGNIFICANCE_LEVEL = 1 - CONFIG["stats"]["confidence_level"]

COUNTRIES_EXPECTED_WEIGHT = {"US": 0.40, "UK": 0.10, "JP": 0.10, "CN": 0.10, "DE": 0.05, "FR": 0.05, "ES": 0.05, "RU": 0.05, "KR": 0.05, "AT": 0.05}
LANGTAGS_EXPECTED_WEIGHT = {"en": 0.50, "ja": 0.10, "zh": 0.10, "de": 0.05, "fr": 0.05, "es": 0.05, "ru": 0.05, "kr": 0.05, "at": 0.05}

WATDIV_BOOST_MU = 0.5
WATDIV_BOOST_SIGMA = 0.5/3.0 

def query(queryfile):
    saveAs = f"{Path(queryfile).parent}/{Path(queryfile).stem}.csv"

    if os.path.exists(saveAs):
        with open(saveAs, "r") as fp:
            header = fp.readline().strip().replace('"', '').split(",")
            result = pd.read_csv(saveAs, parse_dates=[h for h in header if "date" in h])
            return result
    else:
        with open(queryfile, "r") as fp:
            query_text = fp.read()
            _, result = exec_query(query_text, SPARQL_ENDPOINT, error_when_timeout=True)
            header = BytesIO(result).readline().decode().strip().replace('"', '').split(",")
            result = pd.read_csv(BytesIO(result), parse_dates=[h for h in header if "date" in h])
            result.to_csv(saveAs, index=False)

        return result

def dist_test(data: pd.Series, dist: str, figname=None, **kwargs):
    """Test whether the a sample follows normal distribution

        One sample, two-sided Kolmogorov-Smirnov for goodness of fit :
        H0: The test sample is drawn from normal distribution (equal mean)
        H1: The test sample is not drawn from normal distribution (different mean)
        pvalue < alpha = reject H0

    Args:
        data (pd.Series): [description]
        figname ([type], optional): [description]. Defaults to None.

    Returns:
        [type]: [description]
    """

    dist_dict = {
        "norm": lambda x: norm.cdf(x, loc=kwargs["loc"], scale=kwargs["scale"]),
        "uniform": lambda x: uniform.cdf(x, loc=kwargs["loc"], scale=kwargs["scale"])
    }

    if isinstance(data, list):
        data = pd.Series(data)

    if not np.issubdtype(data.dtype, np.number):
        data = pd.Series(LabelEncoder().fit_transform(data), name="producers")

    _, pvalue = kstest(data, dist_dict.get(dist))

    if figname is not None and pvalue < STATS_SIGNIFICANCE_LEVEL:
        figfile = f"{figname}.png"
        if not os.path.exists(figfile):
            fig = data.plot(kind="hist", edgecolor="black")
            data.plot(kind="kde", ax=fig, secondary_y=True)
            plt.savefig(figfile)

        plt.close()
    
    return pvalue    

############
## Test suites
############ 

class TestGenerationTemplate(unittest.TestCase):

    def assertListEqual(self, first, second, msg=None) -> None:

        self.assertIsInstance(first, list, msg="First argument should be a list.")

        if isinstance(second, list):
            return super().assertListEqual(first, second, msg)
        else:
            for item in first:
                self.assertEqual(item, second, msg=msg)

    def assertListAlmostEqual(self, first, second, msg, places=None, delta=None):
        self.assertIsInstance(first, list, msg="First argument should be a list.")
        self.assertEqual(len(first), len(second))
        for item1, item2 in zip(first, second):
            self.assertAlmostEqual(item1, item2, msg=msg, places=places, delta=delta)

    def assertListGreater(self, first, second, msg):
        self.assertIsInstance(first, list, msg="First argument should be a list.")

        if isinstance(second, list):
            for item1, item2 in zip(first, second):
                self.assertGreater(item1, item2, msg=msg)
        else:
            for item in first:
                self.assertGreater(item, second, msg)
    
    def assertListGreaterEqual(self, first, second, msg):
        self.assertIsInstance(first, list, msg="First argument should be a list.")

        if isinstance(second, list):
            for item1, item2 in zip(first, second):
                self.assertGreaterEqual(item1, item2, msg=msg)
        else:
            for item in first:
                self.assertGreaterEqual(item, second, msg)
    
    def assertListLess(self, first, second, msg):
        self.assertIsInstance(first, list, msg="First argument should be a list.")

        if isinstance(second, list):
            for item1, item2 in zip(first, second):
                self.assertLess(item1, item2, msg=msg)
        else:
            for item in first:
                self.assertLess(item, second, msg)
    
    def assertListLessEqual(self, first, second, msg):
        self.assertIsInstance(first, list, msg="First argument should be a list.")

        if isinstance(second, list):
            for item1, item2 in zip(first, second):
                self.assertLessEqual(item1, item2, msg=msg)
        else:
            for item in first:
                self.assertLessEqual(item, second, msg)
    
    def assertInterval(self, a, lower, upper, msg):
        self.assertGreaterEqual(a, lower, msg=msg)
        self.assertLessEqual(a, upper, msg=msg)

    def assertListInterval(self, list1, lower, upper, msg):
        self.assertEqual(len(list1), len(lower), msg="List should have the same length as lower bounds")
        self.assertEqual(len(list1), len(upper), msg="List should have the same length as upper bounds")

        for item, l, u in zip(list1, lower, upper):
            self.assertInterval(item, lower=l, upper=u, msg=msg)


class TestGenerationGlobal(TestGenerationTemplate):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        os.system(f"rm {WORKDIR}/global/*.png")
        os.system(f"rm {WORKDIR}/global/*.csv")    

    def test_global_langtags(self):
        """Test whether langtags across the dataset matches expected frequencies .

        z-test for proportion:
        H0: the proportion for <langtag> is as <expected>
        H1: the proportion for <langtag> is other than <expected>

        """

        queryfile = f"{WORKDIR}/global/test_global_langtags.sparql"
        result = query(queryfile)
        self.assertFalse(result.empty, "The test query should return results...")

        result.replace("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/", "", regex=True, inplace=True)

        frequencies = result["lang"].value_counts(normalize=True).sort_index()
        expected_prop = pd.Series(LANGTAGS_EXPECTED_WEIGHT).loc[frequencies.index.values]
        nbStringLiterals = frequencies.sum()

        _, pvalue, _ = proportions_chisquare(frequencies, nbStringLiterals, expected_prop)

        self.assertGreaterEqual(
            pvalue, STATS_SIGNIFICANCE_LEVEL, 
            msg=f"The proportion for language tags should match {LANGTAGS_EXPECTED_WEIGHT}. Either (1) increase sample size, (2) decrease confidence level or (3) change alternative hypothesis"
        )
            

    def test_global_countries(self):
        """Test whether countroes across the dataset matches expected frequencies .
        """

        tolerance = 0.08

        queryfile = f"{WORKDIR}/global/test_global_countries.sparql"
        result = query(queryfile)
        self.assertFalse(result.empty, "The test query should return results...")

        result.replace("http://downlode.org/rdf/iso-3166/countries#", "", regex=True, inplace=True)

        frequencies = result["country"].value_counts(normalize=True).sort_index()
        expected_proportion = pd.Series(COUNTRIES_EXPECTED_WEIGHT).loc[frequencies.index.values]
        nbCountries = frequencies.sum()

        _, pvalue, _ = proportions_chisquare(frequencies, nbCountries, expected_proportion)

        self.assertGreaterEqual(
            pvalue, STATS_SIGNIFICANCE_LEVEL, 
            msg=f"The proportion for countries should match {COUNTRIES_EXPECTED_WEIGHT}. Either (1) increase sample size, (2) decrease confidence level or (3) change alternative hypothesis"
        )

class TestGenerationProduct(TestGenerationTemplate):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        os.system(f"rm {WORKDIR}/product/*.png")
        os.system(f"rm {WORKDIR}/product/*.csv")    

    def test_product_rel_feature(self):
        """Test whether the Product-ProductFeature is Many to Many and ProductFeature-Product is Many to Many
        """

        queryfile = f"{WORKDIR}/product/test_product_nb_feature.sparql"
        result = query(queryfile)
        self.assertFalse(result.empty, "The test query should return results...")

        result.replace("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/", "", regex=True, inplace=True)

        relation_lhs = result["groupProductFeature"] \
            .apply(lambda x: x.split("|")) \
            .apply(lambda x: np.unique(x).size)

        relation_lhs.to_csv(f"{Path(queryfile).parent}/test_product_rel_nb_feature.csv")

        self.assertListGreaterEqual(
            relation_lhs.to_list(), 1,
            "Every product should have 1..n ProductFeature"
        )

        relation_rhs = result.explode("groupProductFeature").groupby("groupProductFeature")["localProduct"].count()

        self.assertListGreaterEqual(
            relation_rhs.to_list(), 1,
            "Every producer should have 1..n ProductFeature"
        )
    
    def test_product_dist_nb_feature(self):
        """Test whether the features across products follows normal distribution

        D’Agostino and Pearson’s method:
        H0: The test sample is drawn from normal distribution
        H0: The test sample is not drawn from normal distribution
        pvalue < alpha = reject H0

        """

        queryfile = f"{WORKDIR}/product/test_product_nb_feature.sparql"
        result = query(queryfile)
        self.assertFalse(result.empty, "The test query should return results...")

        result.replace("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/", "", regex=True, inplace=True)

        result["groupProductFeature"] = result["groupProductFeature"] \
            .apply(lambda x: x.split("|")) \
            .apply(lambda x: np.unique(x).size)

        normal_test_result = dist_test(result["groupProductFeature"], "norm", loc=WATDIV_BOOST_MU, scale=WATDIV_BOOST_SIGMA, figname=f"{Path(queryfile).parent}/test_product_nb_feature")
        pd.DataFrame([normal_test_result], columns=["pvalue"], index=["groupProductFeature"]).to_csv(f"{Path(queryfile).parent}/test_product_nb_feature_normaltest.csv")
        
        self.assertGreaterEqual(
            normal_test_result, STATS_SIGNIFICANCE_LEVEL,
            "ProductFeature should follow Normal Distribution across Product. Either (1) increase sample size, (2) decrease confidence level or (3) rely on visual check."
        )

    def test_product_rel_producer(self):
        """Test whether the relationship Product-Producer is Many to One and Producer-Product is One to Many.
        """

        queryfile = f"{WORKDIR}/product/test_product_nb_producer.sparql"
        result = query(queryfile)
        self.assertFalse(result.empty, "The test query should return results...")

        result.replace("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/", "", regex=True, inplace=True)
        
        relation_lhs = result["groupProducer"] \
            .apply(lambda x: x.split("|")) \
            .apply(lambda x: np.unique(x).size)

        relation_lhs.to_csv(f"{Path(queryfile).parent}/test_rel_product_nb_producer.csv")

        self.assertListEqual(
            relation_lhs.to_list(), 1,
            "Every product should have 1 producer"
        )

        relation_rhs = result.explode("groupProducer").groupby("groupProducer")["localProduct"].count()

        self.assertListGreaterEqual(
            relation_rhs.to_list(), 1,
            "Every producer should have 1..n products"
        )
        
    def test_product_dist_nb_producer(self):
        """Test whether the number of producers follows normal distribution .
        """

        queryfile = f"{WORKDIR}/product/test_product_nb_producer.sparql"
        result = query(queryfile)
        self.assertFalse(result.empty, "The test query should return results...")

        result.replace("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/", "", regex=True, inplace=True)

        data = np.arange(CONFIG["schema"]["vendor"]["params"]["vendor_n"])
        _, edges = np.histogram(data, CONFIG["n_batch"])
        edges = edges[1:].astype(int)

        result["batchId"] = result["batchId"].apply(lambda x: np.argwhere((x <= edges)).min().item())
        result["groupProducer"] = result["groupProducer"].apply(lambda x: x.split("|"))

        group_producer_by_batches = result.groupby("batchId")["groupProducer"] \
            .aggregate(lambda x: np.concatenate(x.to_numpy())) \
            .to_frame("groupProducer") \
            .reset_index()

        normal_test_result = group_producer_by_batches.apply(
            lambda row: dist_test(row["groupProducer"], "norm", loc=WATDIV_BOOST_MU, scale=WATDIV_BOOST_SIGMA, figname=f"{Path(queryfile).parent}/{Path(queryfile).stem}_batch{row['batchId']}"), 
            axis=1
        )
        
        normal_test_result \
            .to_frame("pvalue").set_index(group_producer_by_batches["batchId"]) \
            .to_csv(f"{Path(queryfile).parent}/test_product_nb_producer_normaltest.csv")

        self.assertListGreaterEqual(
            normal_test_result.to_list(), STATS_SIGNIFICANCE_LEVEL,
            "Producers should follow Normal Distribution for each batch. Either (1) increase sample size, (2) decrease confidence level or (3) rely on visual check."
        )

    def test_product_numeric_props_range(self):
        """Test whether productPropertyNumeric matches expected frequencies .
        """

        queryfile = f"{WORKDIR}/product/test_product_numeric_props.sparql"
        result = query(queryfile)
        self.assertFalse(result.empty, "The test query should return results...")

        result.replace("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/", "", regex=True, inplace=True)

        minVals = result.groupby("prop")["propVal"].min()
        maxVals = result.groupby("prop")["propVal"].max()

        expected_data = pd.DataFrame.from_dict({
            "productPropertyNumeric1": {"min": 1, "max": 2000},
            "productPropertyNumeric2": {"min": 1, "max": 2000},
            "productPropertyNumeric3": {"min": 1, "max": 2000},
            "productPropertyNumeric4": {"min": 1, "max": 2000},
            "productPropertyNumeric5": {"min": 1, "max": 2000}
        }).T
        
        self.assertListGreaterEqual(
            minVals.to_list(), expected_data["min"].to_list(),
            "The min value for productPropertyNumeric must be greater or equal to WatDiv config's ."
        )
            
        self.assertListLessEqual(
            maxVals.to_list(), expected_data["max"].to_list(),
            "The max value for productPropertyNumeric must be less or equal to WatDiv config's ."
        )

    def test_product_numeric_props_frequency(self):
        """Test whether productPropertyNumeric approximately matches expected frequencies .
        """

        queryfile = f"{WORKDIR}/product/test_product_numeric_props.sparql"
        result = query(queryfile)
        self.assertFalse(result.empty, "The test query should return results...")
        result.replace("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/", "", regex=True, inplace=True)

        expected_proportion = pd.Series({
            "productPropertyNumeric1": 1.0,
            "productPropertyNumeric2": 1.0,
            "productPropertyNumeric3": 1.0,
            "productPropertyNumeric4": CONFIG["schema"]["product"]["params"]["productPropertyNumeric4_p"],
            "productPropertyNumeric5": CONFIG["schema"]["product"]["params"]["productPropertyNumeric5_p"]
        })

        frequencies = result["prop"].value_counts().loc[expected_proportion.index.values]
        nbProps = result["localProduct"].nunique()
        
        print(frequencies/nbProps)
                
        for expected_p, freq in zip(expected_proportion, frequencies):
            _, pvalue = proportions_ztest(
                count=int(expected_p * nbProps),
                nobs=nbProps,
                value=(freq / nbProps),
                alternative="larger" # two-sided, smaller, larger
            )
            
            self.assertGreaterEqual(
                pvalue, STATS_SIGNIFICANCE_LEVEL,
                msg="The proportion for bsbm:productPropertyNumeric1..n should match config's. Either (1) increase sample size, (2) decrease confidence level or (3) change alternative hypothesis"
            )
                
    def test_product_numeric_props_normal(self):
        """Test whether productPropertyNumeric follows Normal distribution .
        """

        queryfile = f"{WORKDIR}/product/test_product_numeric_props.sparql"
        result = query(queryfile)
        self.assertFalse(result.empty, "The test query should return results...")

        result.replace("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/", "", regex=True, inplace=True)

        normal_test_data = result.groupby("prop")["propVal"].aggregate(list).to_frame("propVal").reset_index()
        normal_test_result = normal_test_data.apply(
            lambda row: dist_test(row["propVal"], "norm", loc=WATDIV_BOOST_MU, scale=WATDIV_BOOST_SIGMA, figname=f"{Path(queryfile).parent}/{Path(queryfile).stem}_productPropertyNumeric{row['prop']}"), 
            axis=1
        )
        
        normal_test_result \
            .to_frame("pvalue").set_index(normal_test_data["prop"]) \
            .to_csv(f"{Path(queryfile).parent}/test_product_numeric_props_normaltest.csv")

        self.assertListGreaterEqual(
            normal_test_result.to_list(), STATS_SIGNIFICANCE_LEVEL,
            "productPropertyNumeric should follow Normal Distribution for each batch. Either (1) increase sample size, (2) decrease confidence level or (3) rely on visual check."
        )

    def test_product_textual_props_frequency(self):
        """Test whether productPropertyTextual approximately matches expected frequencies .
        """

        queryfile = f"{WORKDIR}/product/test_product_textual_props.sparql"
        result = query(queryfile)
        self.assertFalse(result.empty, "The test query should return results...")

        result.replace("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/", "", regex=True, inplace=True)

        expected_prop = pd.Series({
            "productPropertyTextual1": 1.0,
            "productPropertyTextual2": 1.0,
            "productPropertyTextual3": 1.0,
            "productPropertyTextual4": CONFIG["schema"]["product"]["params"]["productPropertyTextual4_p"],
            "productPropertyTextual5": CONFIG["schema"]["product"]["params"]["productPropertyTextual5_p"]
        })

        frequencies = result["prop"].value_counts().loc[expected_prop.index.values]
        nbProducts = result["localProduct"].nunique()
            
        for expected_p, freq in zip(expected_prop, frequencies):
            _, pvalue = proportions_ztest(
                count=int(expected_p * nbProducts),
                nobs=nbProducts,
                value=(freq / nbProducts),
                alternative="larger" # two-sided, smaller, larger
            )
            print(pvalue)
            
            _, pvalue, _ = proportions_chisquare(freq, nbProducts, expected_p)
            print(pvalue)
            
            self.assertGreaterEqual(
                pvalue, STATS_SIGNIFICANCE_LEVEL,
                msg="The proportion for bsbm:productPropertyTextual1..n should match config's. Either (1) increase sample size, (2) decrease confidence level or (3) change alternative hypothesis"
            )
                
    def test_product_textual_props_normal(self):
        """Test whether productPropertyTextual follows Normal distribution .
        """

        queryfile = f"{WORKDIR}/product/test_product_textual_props.sparql"
        result = query(queryfile)
        self.assertFalse(result.empty, "The test query should return results...")

        result.replace("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/", "", regex=True, inplace=True)

        normal_test_data = result.groupby("prop")["propVal"].aggregate(list).to_frame("propVal").reset_index()
        normal_test_result = normal_test_data.apply(
            lambda row: dist_test(row["propVal"], "norm", loc=WATDIV_BOOST_MU, scale=WATDIV_BOOST_SIGMA, figname=f"{Path(queryfile).parent}/{Path(queryfile).stem}productPropertyTextual{row['prop']}"), 
            axis=1
        )
        
        normal_test_result \
            .to_frame("pvalue").set_index(normal_test_data["prop"]) \
            .to_csv(f"{Path(queryfile).parent}/test_product_textual_props_normaltest.csv")

        self.assertListGreaterEqual(
            normal_test_result.to_list(), STATS_SIGNIFICANCE_LEVEL,
            "productPropertyTextual should follow Normal Distribution for each batch. Either (1) increase sample size, (2) decrease confidence level or (3) rely on visual check."
        )

class TestGenerationVendor(TestGenerationTemplate):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        os.system(f"rm {WORKDIR}/vendor/*.png")
        os.system(f"rm {WORKDIR}/vendor/*.csv")    

    def test_offer_rel_product(self):
        """Test whether the relationship Offer-Product is Many to One and Product-Offer is One to Many
        """

        queryfile = f"{WORKDIR}/vendor/test_vendor_nb_product.sparql"
        result = query(queryfile)
        self.assertFalse(result.empty, "The test query should return results...")

        result.replace("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/", "", regex=True, inplace=True)

        result["groupProduct"] = result["groupProduct"].apply(lambda x: x.split("|"))
        result =  result \
            .groupby("localOffer")["groupProduct"].aggregate(lambda x: np.concatenate(x.to_numpy())) \
            .reset_index()

        relation_lhs = result["groupProduct"].apply(lambda x: np.unique(x).size)

        relation_lhs.to_csv(f"{Path(queryfile).parent}/test_vendor_rel_nb_product.csv")

        self.assertListEqual(
            relation_lhs.to_list(), 1,
            "Every Offer should have 1 product"
        )

        relation_rhs = result.explode("groupProduct").groupby("groupProduct")["localOffer"].count()

        self.assertListGreaterEqual(
            relation_rhs.to_list(), 1,
            "Every Product should have 1..n Offer"
        )
    
    def test_vendor_dist_nb_product(self):
        """Test whether the products across vendor follows normal distribution

        D’Agostino and Pearson’s method:
        H0: The test sample is drawn from normal distribution
        H0: The test sample is not drawn from normal distribution
        pvalue < alpha = reject H0

        """

        queryfile = f"{WORKDIR}/vendor/test_vendor_nb_product.sparql"
        result = query(queryfile)
        self.assertFalse(result.empty, "The test query should return results...")

        result.replace("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/", "", regex=True, inplace=True)

        result["groupProduct"] = result["groupProduct"] \
            .apply(lambda x: x.split("|")) 

        sample = result.groupby("offerId")["groupProduct"].count()   

        normal_test_result = dist_test(sample, "norm", loc=WATDIV_BOOST_MU, scale=WATDIV_BOOST_SIGMA, figname=f"{Path(queryfile).parent}/test_vendor_dist_nb_product")
        pd.DataFrame([normal_test_result], columns=["pvalue"], index=["groupProduct"]).to_csv(f"{Path(queryfile).parent}/test_vendor_dist_nb_product_normaltest.csv")
        
        self.assertGreaterEqual(
            normal_test_result, STATS_SIGNIFICANCE_LEVEL,
            "Products should follow Normal Distribution across vendors. Either (1) increase sample size, (2) decrease confidence level or (3) rely on visual check."
        )
    
    def test_vendor_rel_offer(self):
        """Test whether the relationship Vendor-Offer is One to Many, and Offer-Vendor is Many to One.
        """

        queryfile = f"{WORKDIR}/vendor/test_vendor_nb_offer.sparql"
        result = query(queryfile)
        self.assertFalse(result.empty, "The test query should return results...")

        result.replace("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/", "", regex=True, inplace=True)
        
        relation_lhs = result.groupby("localOffer")["vendorId"].nunique()
        relation_lhs.to_csv(f"{Path(queryfile).parent}/test_vendor_rel_nb_offer_lhs.csv")

        self.assertListEqual(
            relation_lhs.to_list(), 1,
            "Every Offer should have 1 Vendor"
        )

        relation_rhs = result.groupby("vendorId")["localOffer"].nunique()
        relation_rhs.to_csv(f"{Path(queryfile).parent}/test_vendor_rel_nb_offer_rhs.csv")

        self.assertListGreaterEqual(
            relation_rhs.to_list(), 1,
            "Every Vendor should have 1..n Offer"
        )
    
    def test_vendor_dist_nb_offer(self):
        """Test whether the products across vendor follows normal distribution

        D’Agostino and Pearson’s method:
        H0: The test sample is drawn from normal distribution
        H0: The test sample is not drawn from normal distribution
        pvalue < alpha = reject H0

        """

        queryfile = f"{WORKDIR}/vendor/test_vendor_nb_offer.sparql"
        result = query(queryfile)
        self.assertFalse(result.empty, "The test query should return results...")

        result.replace("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/", "", regex=True, inplace=True)

        sample = result.groupby("vendorId")["localOffer"].nunique()

        normal_test_result = dist_test(sample, "norm", loc=WATDIV_BOOST_MU, scale=WATDIV_BOOST_SIGMA, figname=f"{Path(queryfile).parent}/test_vendor_dist_nb_offer")
        pd.DataFrame([normal_test_result], columns=["pvalue"], index=["groupOffer"]).to_csv(f"{Path(queryfile).parent}/test_vendor_dist_nb_offer_normaltest.csv")
        
        self.assertGreaterEqual(
            normal_test_result, STATS_SIGNIFICANCE_LEVEL,
            "Offers should follow Normal Distribution across vendors. Either (1) increase sample size, (2) decrease confidence level or (3) rely on visual check."
        )

    def test_vendor_nb_vendor(self):
        """Test whether the number of vendor per batch match expected .
        """

        data = np.arange(CONFIG["schema"]["vendor"]["params"]["vendor_n"])
        _, edges = np.histogram(data, CONFIG["n_batch"])
        edges = edges[1:].astype(int)

        result = query(f"{WORKDIR}/vendor/test_vendor_nb_vendor.sparql")
        result["batchId"] = result["batchId"].apply(lambda x: np.argwhere((x <= edges)).min().item())
        
        nbVendor = result.groupby("batchId")["nbVendor"].sum().cumsum()

        expected = edges + 1

        for i, test in nbVendor.items():
            self.assertEqual(test, expected[i])

class TestGenerationRatingSite(TestGenerationTemplate):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        os.system(f"rm {WORKDIR}/ratingsite/*.png")
        os.system(f"rm {WORKDIR}/ratingsite/*.csv")    

    def test_ratingsite_rel_nb_product(self):
        """Test whether the products per ratingsite follows normal distribution
        """

        queryfile = f"{WORKDIR}/ratingsite/test_ratingsite_nb_product.sparql"
        result = query(queryfile)
        self.assertFalse(result.empty, "The test query should return results...")

        result.replace("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/", "", regex=True, inplace=True)
        result["groupProduct"] = result["groupProduct"].apply(lambda x: x.split("|"))
        
        relation_lhs = result["groupProduct"].apply(lambda x: np.unique(x).size)
        relation_lhs.to_csv(f"{Path(queryfile).parent}/test_ratingsite_rel_nb_product_lhs.csv")

        self.assertListEqual(
            relation_lhs.to_list(), 1,
            "Every Review should have 1 Product"
        )

        relation_rhs = result.explode("groupProduct").groupby("groupProduct")["localRatingSite"].nunique()
        relation_rhs.to_csv(f"{Path(queryfile).parent}/test_ratingsite_rel_nb_product_rhs.csv")

        self.assertListGreaterEqual(
            relation_rhs.to_list(), 1,
            "Every Product should have 1..n Review"
        )
    
    def test_ratingsite_dist_nb_product(self):
        """Test whether the products across ratingsite follows normal distribution

        D’Agostino and Pearson’s method:
        H0: The test sample is drawn from normal distribution
        H0: The test sample is not drawn from normal distribution
        pvalue < alpha = reject H0

        """

        queryfile = f"{WORKDIR}/ratingsite/test_ratingsite_nb_product.sparql"
        result = query(queryfile)
        self.assertFalse(result.empty, "The test query should return results...")

        result.replace("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/", "", regex=True, inplace=True)

        result["groupProduct"] = result["groupProduct"] \
            .apply(lambda x: x.split("|"))

        normal_sample = result.groupby("localRatingSite")["groupProduct"]\
            .aggregate(lambda x: np.concatenate(x.to_numpy())) \
            .apply(np.unique).apply(len)
        
        normal_test_result = dist_test(normal_sample, "norm", loc=WATDIV_BOOST_MU, scale=WATDIV_BOOST_SIGMA, figname=f"{Path(queryfile).parent}/test_ratingsite_dist_nb_product")
        pd.DataFrame([normal_test_result], columns=["pvalue"], index=["groupProduct"]).to_csv(f"{Path(queryfile).parent}/test_ratingsite_dist_nb_product_normaltest.csv")
        
        self.assertGreaterEqual(
            normal_test_result, STATS_SIGNIFICANCE_LEVEL,
            "Products should follow Normal Distribution across vendors. Either (1) increase sample size, (2) decrease confidence level or (3) rely on visual check."
        )
    
    def test_ratingsite_rel_nb_review(self):
        """Test whether the relationship RatingSite-Review is One-To-Many and Review-RatingSite is Many-To-One.
        """

        queryfile = f"{WORKDIR}/ratingsite/test_ratingsite_nb_review.sparql"
        result = query(queryfile)
        self.assertFalse(result.empty, "The test query should return results...")

        result.replace("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/", "", regex=True, inplace=True)

        result["groupReview"] = result["groupReview"] \
            .apply(lambda x: x.split("|"))
        
        relation_lhs = result["groupReview"].apply(lambda x: np.unique(x).size)

        relation_lhs.to_csv(f"{Path(queryfile).parent}/test_rel_product_nb_producer.csv")

        self.assertListGreaterEqual(
            relation_lhs.to_list(), 1,
            "Every RatingSite should have 1..n Review"
        )

        relation_rhs = result.explode("groupReview").groupby("groupReview")["ratingsiteId"].nunique()

        self.assertListEqual(
            relation_rhs.to_list(), 1,
            "Every Review should have 1 RatingSite"
        )
    
    def test_ratingsite_dist_nb_review(self):
        """Test whether the products across ratingsite follows normal distribution

        D’Agostino and Pearson’s method:
        H0: The test sample is drawn from normal distribution
        H0: The test sample is not drawn from normal distribution
        pvalue < alpha = reject H0

        """

        queryfile = f"{WORKDIR}/ratingsite/test_ratingsite_nb_review.sparql"
        result = query(queryfile)
        self.assertFalse(result.empty, "The test query should return results...")

        result.replace("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/", "", regex=True, inplace=True)

        result["groupReview"] = result["groupReview"] \
            .apply(lambda x: x.split("|")) \
            .apply(lambda x: np.unique(x).size)

        normal_test_result = dist_test(result["groupReview"], "norm", loc=WATDIV_BOOST_MU, scale=WATDIV_BOOST_SIGMA, figname=f"{Path(queryfile).parent}/test_ratingsite_nb_review_across_ratingsite")
        pd.DataFrame([normal_test_result], columns=["pvalue"], index=["groupReview"]).to_csv(f"{Path(queryfile).parent}/test_ratingsite_nb_review_across_ratingsite_normaltest.csv")
        
        self.assertGreaterEqual(
            normal_test_result, STATS_SIGNIFICANCE_LEVEL,
            "Review should follow Normal Distribution across vendors. Either (1) increase sample size, (2) decrease confidence level or (3) rely on visual check."
        )

    def test_ratingsite_rel_nb_person(self):
        """Test whether the reviews per ratingsite follows normal distribution.
        """

        queryfile = f"{WORKDIR}/ratingsite/test_ratingsite_nb_person.sparql"
        result = query(queryfile)
        self.assertFalse(result.empty, "The test query should return results...")

        result.replace("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/", "", regex=True, inplace=True)

        result["groupReviewer"] = result["groupReviewer"] \
            .apply(lambda x: x.split("|"))
                
        relation_lhs = result["groupReviewer"].apply(lambda x: np.unique(x).size)

        relation_lhs.to_csv(f"{Path(queryfile).parent}/test_ratingsite_nb_person.csv")

        self.assertListGreaterEqual(
            relation_lhs.to_list(), 1,
            "Every RatingSite should have 1..n Reviewer"
        )

        relation_rhs = result.explode("groupReviewer").groupby("groupReviewer")["ratingsiteId"].nunique()

        self.assertListEqual(
            relation_rhs.to_list(), 1,
            "Every Reviewer should have 1 RatingSite"
        )
    
    def test_ratingsite_dist_nb_person(self):
        """Test whether the person across ratingsite follows normal distribution

        D’Agostino and Pearson’s method:
        H0: The test sample is drawn from normal distribution
        H0: The test sample is not drawn from normal distribution
        pvalue < alpha = reject H0

        """

        queryfile = f"{WORKDIR}/ratingsite/test_ratingsite_nb_person.sparql"
        result = query(queryfile)
        self.assertFalse(result.empty, "The test query should return results...")

        result.replace("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/", "", regex=True, inplace=True)

        result["groupReviewer"] = result["groupReviewer"] \
            .apply(lambda x: x.split("|")) \
            .apply(lambda x: np.unique(x).size)

        normal_test_result = dist_test(result["groupReviewer"], "norm", loc=WATDIV_BOOST_MU, scale=WATDIV_BOOST_SIGMA, figname=f"{Path(queryfile).parent}/test_ratingsite_nb_person_across_ratingsite")
        pd.DataFrame([normal_test_result], columns=["pvalue"], index=["groupReviewer"]).to_csv(f"{Path(queryfile).parent}/test_ratingsite_nb_person_across_ratingsite_normaltest.csv")
        
        self.assertGreaterEqual(
            normal_test_result, STATS_SIGNIFICANCE_LEVEL,
            "Person should follow Normal Distribution across vendors. Either (1) increase sample size, (2) decrease confidence level or (3) rely on visual check."
        )
        
    def test_ratingsite_nb_ratingsite(self):
        """Test whether the number of RatingSite per batch matches expectation .
        """

        data = np.arange(CONFIG["schema"]["ratingsite"]["params"]["ratingsite_n"])
        _, edges = np.histogram(data, CONFIG["n_batch"])
        edges = edges[1:].astype(int)

        result = query(f"{WORKDIR}/ratingsite/test_ratingsite_nb_ratingsite.sparql")
        result["batchId"] = result["batchId"].apply(lambda x: np.argwhere((x <= edges)).min().item())
        result["nbRatingSite"] = result["nbRatingSite"].apply(lambda x: x.split("|")).apply(np.unique).apply(len)
        nbRatingSite = result.groupby("batchId")["nbRatingSite"].sum().cumsum()

        expected = edges + 1

        for i, test in nbRatingSite.items():
            self.assertEqual(test, expected[i])
    
    def test_ratingsite_ratings_range(self):
        """Test whether productPropertyNumeric values range from 1 to 10 .
        """

        queryfile = f"{WORKDIR}/ratingsite/test_ratingsite_ratings.sparql"
        result = query(queryfile)
        self.assertFalse(result.empty, "The test query should return results...")

        result.replace("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/", "", regex=True, inplace=True)

        minVals = result.groupby("prop")["propVal"].min()
        maxVals = result.groupby("prop")["propVal"].max()

        expected_data = pd.DataFrame.from_dict({
            "rating1": {"min": 1, "max": 10},
            "rating2": {"min": 1, "max": 10},
            "rating3": {"min": 1, "max": 10},
            "rating4": {"min": 1, "max": 10}
        }).T
        
        self.assertListGreaterEqual(
            minVals.to_list(), expected_data["min"].to_list(),
            "The min value for productPropertyNumeric must be greater or equal to WatDiv config's ."
        )
            
        self.assertListLessEqual(
            maxVals.to_list(), expected_data["max"].to_list(),
            "The max value for productPropertyNumeric must be less or equal to WatDiv config's ."
        )

    def test_ratingsite_ratings_frequency(self):
        """Test whether productPropertyNumeric approximately matches expected frequencies .

            Use 1-sample two-sided proportion_ztest to test for proportion:
            - H0: the sample proportion matches expected proportion.
            - H1: the sample proportion is not expected proportion.

            pvalue < alpha: reject H0
        """

        queryfile = f"{WORKDIR}/ratingsite/test_ratingsite_ratings.sparql"
        result = query(queryfile)
        self.assertFalse(result.empty, "The test query should return results...")

        result.replace("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/", "", regex=True, inplace=True)

        nbProducts = result["localReview"].nunique()
        frequencies = (result.groupby("prop")["propVal"].count())

        expected_prop = pd.Series({
            "rating1": CONFIG["schema"]["ratingsite"]["params"]["rating1_p"],
            "rating2": CONFIG["schema"]["ratingsite"]["params"]["rating2_p"],
            "rating3": CONFIG["schema"]["ratingsite"]["params"]["rating3_p"],
            "rating4": CONFIG["schema"]["ratingsite"]["params"]["rating4_p"],
        })

        for expected_p, freq in zip(expected_prop, frequencies):
            _, pvalue = proportions_ztest(
                count=int(expected_p * nbProducts),
                nobs=nbProducts,
                value=(freq / nbProducts),
                alternative="larger"
            )
            
            self.assertGreaterEqual(
                pvalue, STATS_SIGNIFICANCE_LEVEL,
                msg="The proportion for bsbm:rating1..n should match config's."
            )
                
    def test_ratingsite_ratings_normal(self):
        """Test whether productPropertyNumeric follows Normal distribution .
        """

        queryfile = f"{WORKDIR}/ratingsite/test_ratingsite_ratings.sparql"
        result = query(queryfile)
        self.assertFalse(result.empty, "The test query should return results...")

        result.replace("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/", "", regex=True, inplace=True)

        normal_test_data = result.groupby("prop")["propVal"].aggregate(list).to_frame("propVal").reset_index()
        normal_test_result = normal_test_data.apply(
            lambda row: dist_test(row["propVal"], "norm", loc=WATDIV_BOOST_MU, scale=WATDIV_BOOST_SIGMA, figname=f"{Path(queryfile).parent}/{Path(queryfile).stem}_{row['prop']}"), 
            axis=1
        )
        
        normal_test_result \
            .to_frame("pvalue").set_index(normal_test_data["prop"]) \
            .to_csv(f"{Path(queryfile).parent}/test_ratingsite_ratings_normaltest.csv")

        self.assertListGreaterEqual(
            normal_test_result.to_list(), STATS_SIGNIFICANCE_LEVEL, 
            "Ratings should follow Normal Distribution for each batch. Either (1) increase sample size, (2) decrease confidence level or (3) rely on visual check."
        )

if __name__ == "__main__":
    unittest.main()
    