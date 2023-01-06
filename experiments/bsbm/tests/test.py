from io import BytesIO
import os
from pathlib import Path
import unittest
import click
from click.testing import CliRunner

import pandas as pd
import numpy as np
import scipy as sp
import seaborn as sns

# from matplotlib_terminal import plt
import matplotlib.pyplot as plt

from sklearn.preprocessing import LabelEncoder

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

def normal_test(data: pd.Series, figname=None, **kwargs):

    if isinstance(data, list):
        data = pd.Series(data)

    if not np.issubdtype(data.dtype, np.number):
        data = pd.Series(LabelEncoder().fit_transform(data), name="producers")

    try: 
        stat, pvalue = sp.stats.normaltest(data)
        # print(f"D’Agostino and Pearson’s normal test: stat = {stat}, pvalue = {pvalue}")
        # plt = sns.displot(data, kde=True)
        fig = data.plot(kind="hist", edgecolor="black", **kwargs)
        data.plot(kind="kde", ax=fig, secondary_y=True, **kwargs)
        # plt.show("braille")

        if figname is not None and pvalue >= STATS_SIGNIFICANCE_LEVEL:
            plt.savefig(f"{figname}.png")

        plt.close()

        return pvalue
    except ValueError:
        return np.nan

############
## Test suites
############ 

class TestGenerationGlobal(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        os.system(f"rm {WORKDIR}/global/*.png")
        os.system(f"rm {WORKDIR}/global/*.csv")    

    def assertListAlmostEqual(self, first, second, msg, places=None, delta=None):
        self.assertEqual(len(first), len(second))
        for item1, item2 in zip(first, second):
            self.assertAlmostEqual(item1, item2, msg=msg, places=places, delta=delta)

    def test_global_langtags(self):
        """Test whether langtags across the dataset matches expected frequencies .
        """

        tolerance = 0.03

        queryfile = f"{WORKDIR}/global/test_global_langtags.sparql"
        result = query(queryfile)
        result.replace("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/", "", regex=True, inplace=True)

        data = np.arange(CONFIG["schema"]["vendor"]["params"]["vendor_n"])
        _, edges = np.histogram(data, CONFIG["n_batch"])
        edges = edges[1:].astype(int)
        
        result["batchId"] = result["batchId"].apply(lambda x: np.argwhere((x <= edges)).min().item())

        proportions = result.groupby(["batchId"])["lang"].value_counts(normalize=True).to_frame("proportion") 

        expected_proportion = pd.DataFrame.from_dict(LANGTAGS_EXPECTED_WEIGHT, orient="index", columns=["expected_proportion"])
        expected_proportion.index.name = "lang"

        test = proportions.join(expected_proportion, on=["lang"]).round(2)
        test.to_csv(f"{Path(queryfile).parent}/test_global_langtags_final.csv")
        
        self.assertListAlmostEqual(
            test["proportion"].to_list(), test["expected_proportion"].to_list(),
            delta=tolerance,
            msg="The frequency for language tags should match config's."
        )

    def test_global_countries(self):
        """Test whether countroes across the dataset matches expected frequencies .
        """

        tolerance = 0.04

        queryfile = f"{WORKDIR}/global/test_global_countries.sparql"
        result = query(queryfile)
        result.replace("http://downlode.org/rdf/iso-3166/countries#", "", regex=True, inplace=True)

        data = np.arange(CONFIG["schema"]["vendor"]["params"]["vendor_n"])
        _, edges = np.histogram(data, CONFIG["n_batch"])
        edges = edges[1:].astype(int)
        
        result["batchId"] = result["batchId"].apply(lambda x: np.argwhere((x <= edges)).min().item())

        proportions = result.groupby(["batchId"])["country"].value_counts(normalize=True).to_frame("proportion") 

        expected_proportion = pd.DataFrame.from_dict(COUNTRIES_EXPECTED_WEIGHT, orient="index", columns=["expected_proportion"])
        expected_proportion.index.name = "country"

        test = proportions.join(expected_proportion, on=["country"]).round(2)
        test.to_csv(f"{Path(queryfile).parent}/test_global_countries_final.csv")
        
        self.assertListAlmostEqual(
            test["proportion"].to_list(), test["expected_proportion"].to_list(),
            delta=tolerance,
            msg="The frequency for bsbm:country should match config's."
        )

class TestGenerationProduct(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        os.system(f"rm {WORKDIR}/product/*.png")
        os.system(f"rm {WORKDIR}/product/*.csv")    

    def assertListAlmostEqual(self, first, second, msg, places=None, delta=None):
        self.assertEqual(len(first), len(second))
        for item1, item2 in zip(first, second):
            self.assertAlmostEqual(item1, item2, msg=msg, places=places, delta=delta)

    def test_product_nb_feature_per_product(self):
        """Test whether the features per product follows normal distribution

        D’Agostino and Pearson’s method:
        H0: The test sample is not drawn from normal distribution
        H1: The test sample is drawn from normal distribution
        pvalue < alpha = reject H0

        """

        queryfile = f"{WORKDIR}/product/test_product_nb_feature.sparql"
        result = query(queryfile)

        result["groupProductFeature"] = result["groupProductFeature"] \
            .apply(lambda x: x.split("|"))

        normal_test_result = result.apply(
            lambda row: normal_test(row["groupProductFeature"], figname=f"{Path(queryfile).parent}/test_product_nb_feature_per_product_{row['productHash']}"),
            axis = 1
        )

        normal_test_result \
            .to_frame("pvalue").set_index(result["productHash"]) \
            .to_csv(f"{Path(queryfile).parent}/test_product_nb_feature_per_product_normaltest.csv")
        
        self.assertTrue(
            (normal_test_result < STATS_SIGNIFICANCE_LEVEL).all(),
            "ProductFeatures should follow Normal Distribution for each vendor. Either (1) increase sample size, (2) decrease confidence level or (3) rely on visual check."
        )
    
    def test_product_nb_feature_across_product(self):
        """Test whether the features across products follows normal distribution

        D’Agostino and Pearson’s method:
        H0: The test sample is not drawn from normal distribution
        H1: The test sample is drawn from normal distribution
        pvalue < alpha = reject H0

        """

        queryfile = f"{WORKDIR}/product/test_product_nb_feature.sparql"
        result = query(queryfile)
        result["groupProductFeature"] = result["groupProductFeature"] \
            .apply(lambda x: x.split("|")) \
            .apply(lambda x: np.unique(x).size)
        
        self.assertTrue(
            normal_test(result["groupProductFeature"], figname=f"{Path(queryfile).parent}/test_product_nb_feature_across_product"),
            "Products should follow Normal Distribution across vendors. Either (1) increase sample size, (2) decrease confidence level or (3) rely on visual check."
        )

    def test_product_nb_producer_per_product(self):
        """Test whether the average number of producers per product is 1 .
        """
        result = query(f"{WORKDIR}/product/test_product_nb_producer_per_product.sparql")
        self.assertTrue(
            (result["nbProducer"] == 1).all(),
            "Each product should have only 1 producer"
        )
        
    def test_product_nb_producer(self):
        """Test whether the number of producers follows normal distribution .
        """

        queryfile = f"{WORKDIR}/product/test_product_nb_producer.sparql"
        result = query(queryfile)

        data = np.arange(CONFIG["schema"]["vendor"]["params"]["vendor_n"])
        _, edges = np.histogram(data, CONFIG["n_batch"])
        edges = edges[1:].astype(int)

        result["batchId"] = result["batchId"].apply(lambda x: np.argwhere((x <= edges)).min().item())
        result["groupProducer"] = result["groupProducer"].apply(lambda x: np.unique(x.split("|")))

        group_producer_by_batches = result.groupby("batchId")["groupProducer"] \
            .aggregate(np.concatenate) \
            .to_frame("groupProducer") \
            .reset_index()
        
        normal_test_result = group_producer_by_batches.apply(
            lambda row: normal_test(row["groupProducer"], figname=f"{Path(queryfile).parent}/{Path(queryfile).stem}_batch{row['batchId']}"), 
            axis=1
        )
        
        normal_test_result \
            .to_frame("pvalue").set_index(group_producer_by_batches["batchId"]) \
            .to_csv(f"{Path(queryfile).parent}/test_product_nb_producer_normaltest.csv")

        self.assertTrue(
            (normal_test_result < STATS_SIGNIFICANCE_LEVEL).all(),
            "Producers should follow Normal Distribution for each batch. Either (1) increase sample size, (2) decrease confidence level or (3) rely on visual check."
        )

    def test_product_numeric_props_range(self):
        """Test whether productPropertyNumeric matches expected frequencies .
        """

        queryfile = f"{WORKDIR}/product/test_product_numeric_props.sparql"
        result = query(queryfile)
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
        
        self.assertTrue(
            np.greater_equal(minVals, expected_data["min"]).all(),
            "The min value for productPropertyNumeric must be greater or equal to WatDiv config's ."
        )
            
        self.assertTrue(
            np.less_equal(maxVals, expected_data["max"]).all(),
            "The max value for productPropertyNumeric must be less or equal to WatDiv config's ."
        )

    def test_product_numeric_props_frequency(self):
        """Test whether productPropertyNumeric approximately matches expected frequencies .
        """

        tolerance = 0.1

        queryfile = f"{WORKDIR}/product/test_product_numeric_props.sparql"
        result = query(queryfile)
        result.replace("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/", "", regex=True, inplace=True)

        nbProducts = result["localProduct"].nunique()
        frequencies = (result.groupby("prop")["propVal"].count() / nbProducts).round(2)

        expected_data = {
            "productPropertyNumeric1": 1.0,
            "productPropertyNumeric2": 1.0,
            "productPropertyNumeric3": 1.0,
            "productPropertyNumeric4": CONFIG["schema"]["product"]["params"]["productPropertyNumeric4_p"],
            "productPropertyNumeric5": CONFIG["schema"]["product"]["params"]["productPropertyNumeric5_p"]
        }
        
        self.assertListAlmostEqual(
            frequencies.to_list(), list(expected_data.values()),
            delta=tolerance,
            msg="The frequency for productPropertyNumeric should match config's."
        )
                
    def test_product_numeric_props_normal(self):
        """Test whether productPropertyNumeric follows Normal distribution .
        """

        queryfile = f"{WORKDIR}/product/test_product_numeric_props.sparql"
        result = query(queryfile)
        result.replace("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/", "", regex=True, inplace=True)

        normal_test_data = result.groupby("prop")["propVal"].aggregate(list).to_frame("propVal").reset_index()
        normal_test_result = normal_test_data.apply(
            lambda row: normal_test(row["propVal"], figname=f"{Path(queryfile).parent}/{Path(queryfile).stem}_productPropertyNumeric{row['prop']}"), 
            axis=1
        )
        
        normal_test_result \
            .to_frame("pvalue").set_index(normal_test_data["prop"]) \
            .to_csv(f"{Path(queryfile).parent}/test_product_numeric_props_normaltest.csv")

        self.assertTrue(
            (normal_test_result < STATS_SIGNIFICANCE_LEVEL).all(),
            "productPropertyNumeric should follow Normal Distribution for each batch. Either (1) increase sample size, (2) decrease confidence level or (3) rely on visual check."
        )

    def test_product_textual_props_frequency(self):
        """Test whether productPropertyTextual approximately matches expected frequencies .
        """

        tolerance = 0.1

        queryfile = f"{WORKDIR}/product/test_product_textual_props.sparql"
        result = query(queryfile)
        result.replace("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/", "", regex=True, inplace=True)

        nbProducts = result["localProduct"].nunique()
        frequencies = (result.groupby("prop")["propVal"].count() / nbProducts).round(2)

        expected_data = {
            "productPropertyTextual1": 1.0,
            "productPropertyTextual2": 1.0,
            "productPropertyTextual3": 1.0,
            "productPropertyTextual4": CONFIG["schema"]["product"]["params"]["productPropertyTextual4_p"],
            "productPropertyTextual5": CONFIG["schema"]["product"]["params"]["productPropertyTextual5_p"]
        }
        
        self.assertListAlmostEqual(
            frequencies.to_list(), list(expected_data.values()),
            delta=tolerance,
            msg="The frequency for productPropertyTextual should match config's."
        )
                
    def test_product_textual_props_normal(self):
        """Test whether productPropertyTextual follows Normal distribution .
        """

        queryfile = f"{WORKDIR}/product/test_product_textual_props.sparql"
        result = query(queryfile)
        result.replace("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/", "", regex=True, inplace=True)

        normal_test_data = result.groupby("prop")["propVal"].aggregate(list).to_frame("propVal").reset_index()
        normal_test_result = normal_test_data.apply(
            lambda row: normal_test(row["propVal"], figname=f"{Path(queryfile).parent}/{Path(queryfile).stem}productPropertyTextual{row['prop']}"), 
            axis=1
        )
        
        normal_test_result \
            .to_frame("pvalue").set_index(normal_test_data["prop"]) \
            .to_csv(f"{Path(queryfile).parent}/test_product_textual_props_normaltest.csv")

        self.assertTrue(
            (normal_test_result < STATS_SIGNIFICANCE_LEVEL).all(),
            "productPropertyTextual should follow Normal Distribution for each batch. Either (1) increase sample size, (2) decrease confidence level or (3) rely on visual check."
        )

class TestGenerationVendor(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        os.system(f"rm {WORKDIR}/vendor/*.png")
        os.system(f"rm {WORKDIR}/vendor/*.csv")    

    def assertListAlmostEqual(self, first, second, msg, places=None, delta=None):
        self.assertEqual(len(first), len(second))
        for item1, item2 in zip(first, second):
            self.assertAlmostEqual(item1, item2, msg=msg, places=places, delta=delta)

    def test_vendor_nb_product_per_vendor(self):
        """Test whether the products per vendor follows normal distribution

        D’Agostino and Pearson’s method:
        H0: The test sample is not drawn from normal distribution
        H1: The test sample is drawn from normal distribution
        pvalue < alpha = reject H0

        """

        queryfile = f"{WORKDIR}/vendor/test_vendor_nb_product.sparql"
        result = query(queryfile)

        result["groupProduct"] = result["groupProduct"] \
            .apply(lambda x: x.split("|"))

        normal_test_result = result.apply(
            lambda row: normal_test(row["groupProduct"], figname=f"{Path(queryfile).parent}/test_vendor_nb_product_per_vendor_{row['vendorId']}"),
            axis = 1
        )

        normal_test_result \
            .to_frame("pvalue").set_index(result["vendorId"]) \
            .to_csv(f"{Path(queryfile).parent}/test_vendor_nb_product_per_vendor_normaltest.csv")
        
        self.assertTrue(
            (normal_test_result < STATS_SIGNIFICANCE_LEVEL).all(),
            "Products should follow Normal Distribution for each vendor. Either (1) increase sample size, (2) decrease confidence level or (3) rely on visual check."
        )
    
    def test_vendor_nb_product_across_vendor(self):
        """Test whether the products across vendor follows normal distribution

        D’Agostino and Pearson’s method:
        H0: The test sample is not drawn from normal distribution
        H1: The test sample is drawn from normal distribution
        pvalue < alpha = reject H0

        """

        queryfile = f"{WORKDIR}/vendor/test_vendor_nb_product.sparql"
        result = query(queryfile)
        result["groupProduct"] = result["groupProduct"] \
            .apply(lambda x: x.split("|")) \
            .apply(lambda x: np.unique(x).size)
        
        self.assertTrue(
            normal_test(result["groupProduct"], figname=f"{Path(queryfile).parent}/test_vendor_nb_product_across_vendor"),
            "Products should follow Normal Distribution across vendors. Either (1) increase sample size, (2) decrease confidence level or (3) rely on visual check."
        )
    
    def test_vendor_nb_offer_per_vendor(self):
        """Test whether the offers per vendor follows normal distribution.

        D’Agostino and Pearson’s method:
        H0: The test sample is not drawn from normal distribution
        H1: The test sample is drawn from normal distribution
        pvalue < alpha = reject H0

        """

        queryfile = f"{WORKDIR}/vendor/test_vendor_nb_offer.sparql"
        result = query(queryfile)
        result["groupOffer"] = result["groupOffer"] \
            .apply(lambda x: x.split("|"))
        
        normal_test_result = result.apply(
            lambda row: normal_test(row["groupOffer"], figname=f"{Path(queryfile).parent}/test_vendor_nb_offer_per_vendor_{row['vendorId']}"),
            axis = 1
        )

        normal_test_result \
            .to_frame("pvalue").set_index(result["vendorId"]) \
            .to_csv(f"{Path(queryfile).parent}/test_vendor_nb_offer_per_vendor_normaltest.csv")
        
        self.assertTrue(
            (normal_test_result < STATS_SIGNIFICANCE_LEVEL).all(),
            "Offers should follow Normal Distribution for each vendor. Either (1) increase sample size, (2) decrease confidence level or (3) rely on visual check."
        )
    
    def test_vendor_nb_offer_across_vendor(self):
        """Test whether the products across vendor follows normal distribution

        D’Agostino and Pearson’s method:
        H0: The test sample is not drawn from normal distribution
        H1: The test sample is drawn from normal distribution
        pvalue < alpha = reject H0

        """

        queryfile = f"{WORKDIR}/vendor/test_vendor_nb_offer.sparql"
        result = query(queryfile)
        result["groupOffer"] = result["groupOffer"] \
            .apply(lambda x: x.split("|")) \
            .apply(lambda x: np.unique(x).size)

        normal_test_result = normal_test(result["groupOffer"], figname=f"{Path(queryfile).parent}/test_vendor_nb_product_across_vendor")
        
        self.assertTrue(
            normal_test_result,
            "Offers should follow Normal Distribution across vendors. Either (1) increase sample size, (2) decrease confidence level or (3) rely on visual check."
        )
    
    def test_vendor_nb_vendor(self):
        """Test whether the number of producers follows normal distribution .
        """

        data = np.arange(CONFIG["schema"]["vendor"]["params"]["vendor_n"])
        _, edges = np.histogram(data, CONFIG["n_batch"])
        edges = edges[1:].astype(int)

        result = query(f"{WORKDIR}/vendor/test_vendor_nb_vendor.sparql")
        result["batchId"] = result["batchId"].apply(lambda x: np.argwhere((x <= edges)).min().item())
        
        nbVendor = result.groupby("batchId")["nbVendor"].sum()

        expected = edges + 1

        for i, test in nbVendor.items():
            self.assertEqual(test, expected[i])

class TestGenerationPerson(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        os.system(f"rm {WORKDIR}/person/*.png")
        os.system(f"rm {WORKDIR}/person/*.csv")    

    def assertListAlmostEqual(self, first, second, msg, places=None, delta=None):
        self.assertEqual(len(first), len(second))
        for item1, item2 in zip(first, second):
            self.assertAlmostEqual(item1, item2, msg=msg, places=places, delta=delta)

    def test_person_nb_product_per_person(self):
        """Test whether the products per person follows normal distribution

        D’Agostino and Pearson’s method:
        H0: The test sample is not drawn from normal distribution
        H1: The test sample is drawn from normal distribution
        pvalue < alpha = reject H0

        """

        queryfile = f"{WORKDIR}/person/test_person_nb_product.sparql"
        result = query(queryfile)

        result["groupProduct"] = result["groupProduct"] \
            .apply(lambda x: x.split("|"))

        normal_test_result = result.apply(
            lambda row: normal_test(row["groupProduct"], figname=f"{Path(queryfile).parent}/test_person_nb_product_per_person_{row['personId']}"),
            axis = 1
        )

        normal_test_result \
            .to_frame("pvalue").set_index(result["personId"]) \
            .to_csv(f"{Path(queryfile).parent}/test_person_nb_product_per_person_normaltest.csv")
        
        self.assertTrue(
            (normal_test_result < STATS_SIGNIFICANCE_LEVEL).all(),
            "Products should follow Normal Distribution for each person. Either (1) increase sample size, (2) decrease confidence level or (3) rely on visual check."
        )
    
    def test_person_nb_product_across_person(self):
        """Test whether the products across person follows normal distribution

        D’Agostino and Pearson’s method:
        H0: The test sample is not drawn from normal distribution
        H1: The test sample is drawn from normal distribution
        pvalue < alpha = reject H0

        """

        queryfile = f"{WORKDIR}/person/test_person_nb_product.sparql"
        result = query(queryfile)
        result["groupProduct"] = result["groupProduct"] \
            .apply(lambda x: x.split("|")) \
            .apply(lambda x: np.unique(x).size)
        
        self.assertTrue(
            normal_test(result["groupProduct"], figname=f"{Path(queryfile).parent}/test_person_nb_product_across_person"),
            "Products should follow Normal Distribution across persons. Either (1) increase sample size, (2) decrease confidence level or (3) rely on visual check."
        )
    
    def test_person_nb_review_per_person(self):
        """Test whether the reviews per person follows normal distribution.

        D’Agostino and Pearson’s method:
        H0: The test sample is not drawn from normal distribution
        H1: The test sample is drawn from normal distribution
        pvalue < alpha = reject H0

        """

        queryfile = f"{WORKDIR}/person/test_person_nb_review.sparql"
        result = query(queryfile)
        result["groupReview"] = result["groupReview"] \
            .apply(lambda x: x.split("|"))
        
        normal_test_result = result.apply(
            lambda row: normal_test(row["groupReview"], figname=f"{Path(queryfile).parent}/test_person_nb_review_per_person_{row['personId']}"),
            axis = 1
        )

        normal_test_result \
            .to_frame("pvalue").set_index(result["personId"]) \
            .to_csv(f"{Path(queryfile).parent}/test_person_nb_review_per_person_normaltest.csv")
        
        self.assertTrue(
            (normal_test_result < STATS_SIGNIFICANCE_LEVEL).all(),
            "Offers should follow Normal Distribution for each person. Either (1) increase sample size, (2) decrease confidence level or (3) rely on visual check."
        )
    
    def test_person_nb_review_across_person(self):
        """Test whether the products across person follows normal distribution

        D’Agostino and Pearson’s method:
        H0: The test sample is not drawn from normal distribution
        H1: The test sample is drawn from normal distribution
        pvalue < alpha = reject H0

        """

        queryfile = f"{WORKDIR}/person/test_person_nb_review.sparql"
        result = query(queryfile)
        result["groupReview"] = result["groupReview"] \
            .apply(lambda x: x.split("|")) \
            .apply(lambda x: np.unique(x).size)

        normal_test_result = normal_test(result["groupReview"], figname=f"{Path(queryfile).parent}/test_person_nb_product_across_person")
        
        self.assertTrue(
            normal_test_result,
            "Offers should follow Normal Distribution across persons. Either (1) increase sample size, (2) decrease confidence level or (3) rely on visual check."
        )
    
    def test_person_nb_person(self):
        """Test whether the number of producers follows normal distribution .
        """

        data = np.arange(CONFIG["schema"]["person"]["params"]["person_n"])
        _, edges = np.histogram(data, CONFIG["n_batch"])
        edges = edges[1:].astype(int)

        result = query(f"{WORKDIR}/person/test_person_nb_person.sparql")
        result["batchId"] = result["batchId"].apply(lambda x: np.argwhere((x <= edges)).min().item())
        
        nbPerson = result.groupby("batchId")["nbPerson"].sum()

        expected = edges + 1

        for i, test in nbPerson.items():
            self.assertEqual(test, expected[i])
    
    def test_person_ratings_range(self):
        """Test whether productPropertyNumeric matches expected frequencies .
        """

        queryfile = f"{WORKDIR}/person/test_person_ratings.sparql"
        result = query(queryfile)
        result.replace("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/", "", regex=True, inplace=True)

        minVals = result.groupby("prop")["propVal"].min()
        maxVals = result.groupby("prop")["propVal"].max()

        expected_data = pd.DataFrame.from_dict({
            "rating1": {"min": 1, "max": 10},
            "rating2": {"min": 1, "max": 10},
            "rating3": {"min": 1, "max": 10},
            "rating4": {"min": 1, "max": 10}
        }).T
        
        self.assertTrue(
            np.greater_equal(minVals, expected_data["min"]).all(),
            "The min value for productPropertyNumeric must be greater or equal to WatDiv config's ."
        )
            
        self.assertTrue(
            np.less_equal(maxVals, expected_data["max"]).all(),
            "The max value for productPropertyNumeric must be less or equal to WatDiv config's ."
        )

    def test_person_ratings_frequency(self):
        """Test whether productPropertyNumeric approximately matches expected frequencies .
        """

        tolerance = 0.07

        queryfile = f"{WORKDIR}/person/test_person_ratings.sparql"
        result = query(queryfile)
        result.replace("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/", "", regex=True, inplace=True)

        nbProducts = result["localReview"].nunique()
        frequencies = (result.groupby("prop")["propVal"].count() / nbProducts).round(2)

        expected_data = {
            "rating1": CONFIG["schema"]["person"]["params"]["rating1_p"],
            "rating2": CONFIG["schema"]["person"]["params"]["rating2_p"],
            "rating3": CONFIG["schema"]["person"]["params"]["rating3_p"],
            "rating4": CONFIG["schema"]["person"]["params"]["rating4_p"],
        }
        
        self.assertListAlmostEqual(
            frequencies.to_list(), list(expected_data.values()),
            delta=tolerance,
            msg="The frequency for bsbm:rating1..n should match config's."
        )
                
    def test_person_ratings_normal(self):
        """Test whether productPropertyNumeric follows Normal distribution .
        """

        queryfile = f"{WORKDIR}/person/test_person_ratings.sparql"
        result = query(queryfile)
        result.replace("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/", "", regex=True, inplace=True)

        normal_test_data = result.groupby("prop")["propVal"].aggregate(list).to_frame("propVal").reset_index()
        normal_test_result = normal_test_data.apply(
            lambda row: normal_test(row["propVal"], figname=f"{Path(queryfile).parent}/{Path(queryfile).stem}_{row['prop']}"), 
            axis=1
        )
        
        normal_test_result \
            .to_frame("pvalue").set_index(normal_test_data["prop"]) \
            .to_csv(f"{Path(queryfile).parent}/test_person_ratings_normaltest.csv")

        self.assertTrue(
            (normal_test_result < STATS_SIGNIFICANCE_LEVEL).all(),
            "Ratings should follow Normal Distribution for each batch. Either (1) increase sample size, (2) decrease confidence level or (3) rely on visual check."
        )

if __name__ == "__main__":
    unittest.main()
    