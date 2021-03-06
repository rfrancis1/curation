"""
File is intended to establish a 'HPO class' that can be used
to store data quality metrics for each HPO in an easy and
identifiable fashion.

Class was used as a means for storing information as the ability
to add functions could prove useful in future iterations of the
script.
"""

from dictionaries_and_lists import thresholds
from datetime import date
import sys
import constants


class DataQualityMetric:
    """
    Class is used to store data quality metrics.
    """

    def __init__(
        self, hpo='', table_or_class='', metric_type='', value=0,
            data_quality_dimension='', first_reported=date.today(),
            link=''):

        """
        Used to establish the attributes of the DataQualityMetric
        object being instantiated.

        Parameters
        ----------
        hpo (string): name of the HPO being associated with the
            data quality metric in question (e.g. nyc_cu).

        table_or_class (string): name of the table or class whose
            data quality metric is being determined
            (e.g. Measurement, ACE Inhibitor).

        metric_type (string): name of the metric that is being
            determined (e.g. duplicates).

        value (float): value that represents the quantitative value
            of the data quality metric being investigated.

        data_quality_dimension (string): represents whether the
            metric_type being investigated is related to the
            conformance, completeness, or plausibility of data
            quality with respect to the Kahn framework.

        first_reported (datetime.date): represents the time
            at which this metric (with all of the other parameters
            being exactly the same) was first reported.

        link (string): link to the AoU EHR Operations page that
            can help the site troubleshoot its data quality.
        """

        self.hpo = hpo
        self.table_or_class = table_or_class
        self.metric_type = metric_type
        self.value = value
        self.data_quality_dimension = data_quality_dimension
        self.first_reported = first_reported
        self.link = link

    def print_dqd_attributes(self):
        """
        Function is used to print out some of the attributes
        of a DataQualityMetric object in a manner that enables
        all of the information to be displayed in a
        human-readable format.
        """
        print(
            f"""HPO: {self.hpo}
            Table Or Class: {self.table_or_class}
            Metric Type: {self.metric_type}
            Value: {self.value}
            Data Quality Dimension: {self.data_quality_dimension}
            First Reported: {self.first_reported}
            Link: {self.link}""")

    def get_list_of_attribute_names(self):
        """
        Function is used to get a list of the attributes that
        are associated with a DataQualityMetric object. This will
        ultimately be used to populate the columns of a
        pandas dataframe.

        Return
        ------
        attribute_names (list): list of the attribute names
            for a DataQualityMetric object.
        """

        attribute_names = [
            "HPO", "Table/Class", "Metric Type",
            "Value", "Data Quality Dimension", "First Reported",
            "Link"]

        return attribute_names

    def get_attributes_in_order(self):
        """
        Function is used to get the attributes of a particular
        DataQualityMetric object in an order that parallels
        the get_list_of_attribute_names function above. This
        will be used to populate the dataframe with data quality
        issues.

        Return
        ------
        attributes (list): list of the attributes (values, strings)
            for the object.
        """

        attributes = [
            self.hpo, self.table_or_class, self.metric_type, self.value,
            self.data_quality_dimension, self.first_reported, self.link]

        return attributes


class HPO:
    """
    Class is used to associated data quality issues with a particular
    HPO.
    """

    def __init__(
            self, name, full_name, concept_success, duplicates,
            end_before_begin, data_after_death,
            route_success, unit_success, measurement_integration,
            ingredient_integration, date_datetime_disparity,
            erroneous_dates, person_id_failure_rate,
            visit_date_disparity, visit_id_failure):

        """
        Used to establish the attributes of the HPO object being instantiated.

        Parameters
        ----------
        self (HPO object): the object to be created.

        name (str): name of the HPO ID to create (e.g. nyc_cu).

        full_name (str): full name of the HPO.

        all other optional parameters are intended to be lists. These
        lists should contain DataQualityMetric objects that have all
        of the relevant pieces pertaining to said metric object.

        the exact descriptions of the data quality metrics can be found
        on the AoU HPO website at the following link:
            sites.google.com/view/ehrupload
        """
        self.name = name
        self.full_name = full_name

        # relates to multiple tables - therefore should be list of objects
        self.concept_success = concept_success
        self.duplicates = duplicates
        self.end_before_begin = end_before_begin
        self.data_after_death = data_after_death
        self.date_datetime_disparity = date_datetime_disparity
        self.erroneous_dates = erroneous_dates
        self.person_id_failure_rate = person_id_failure_rate
        self.visit_date_disparity = visit_date_disparity
        self.visit_id_failure = visit_id_failure

        # only relates to one table - therefore single float expected
        self.route_success = route_success
        self.unit_success = unit_success
        self.measurement_integration = measurement_integration
        self.ingredient_integration = ingredient_integration

    def add_attribute_with_string(self, metric, dq_object):
        """
        Function is designed to enable the script to add
        a DataQualityMetric object to the attributes that
        define an HPO object. This will allow us to easily
        associate an HPO object with its constituent data
        quality metrics.

        Parameters
        ----------
        metric (string): the name of the sheet that contains the
            dimension of data quality to be investigated.

        dq_object (DataQualityMetric): object that contains
            the information for a particular aspect of the
            site's data quality (NOTE: dq_object.hpo should
            equal self.name whenever this is used).
        """

        if metric == constants.concept_full:
            self.concept_success.append(dq_object)

        elif metric == constants.duplicates_full:
            self.duplicates.append(dq_object)

        elif metric == constants.end_before_begin_full:
            self.end_before_begin.append(dq_object)

        elif metric == constants.data_after_death_full:
            self.data_after_death.append(dq_object)

        elif metric == constants.sites_measurement_full:
            self.measurement_integration.append(dq_object)

        elif metric == constants.drug_success_full:
            self.ingredient_integration.append(dq_object)

        elif metric == constants.drug_routes_full:
            self.route_success.append(dq_object)

        elif metric == constants.unit_success_full:
            self.unit_success.append(dq_object)

        elif metric == constants.date_datetime_disparity_full:
            self.date_datetime_disparity.append(dq_object)

        elif metric == constants.erroneous_dates_full:
            self.erroneous_dates.append(dq_object)

        elif metric == constants.person_id_failure_rate_full:
            self.erroneous_dates.append(dq_object)

        elif metric == constants.visit_date_disparity_full:
            self.visit_date_disparity.append(dq_object)

        elif metric == constants.visit_id_failure_rate_full:
            self.visit_id_failure.append(dq_object)

        else:
            hpo_name = self.name

            print(f"Unrecognized metric input: {metric} for {hpo_name}")
            sys.exit(0)

    def find_failing_metrics(self):
        """
        Function is used to create a catalogue of the 'failing' data
        quality metrics at defined by the thresholds established by
        the appropriate dictionary from relevant_dictionaries.

        Parameters
        ----------
        self (HPO object): the object whose 'failing metrics' are to
            be determined

        Returns
        -------
        failing_metrics (list): has a list of the data quality metrics
            for the HPO that have 'failed' based on the thresholds
            provided

        NOTES
        -----
        1. if no data quality problems are found, however, the
        function will return 'None' to signify that no issues arose

        2. this funciton is not currently implemented in our current
        iteration of metrics_over_time. this function, however, holds
        potential to be useful in future iterations.
        """

        failing_metrics = []

        # below we can find the data quality metrics for several tables -
        # need to iterate through a list to get the objects for each table
        for concept_success_obj in self.concept_success:
            if concept_success_obj.value < thresholds[constants.concept_success_min]:
                failing_metrics.append(concept_success_obj)

        for duplicates_obj in self.duplicates:
            if duplicates_obj.value > thresholds[constants.duplicates_max]:
                failing_metrics.append(duplicates_obj)

        for end_before_begin_obj in self.end_before_begin:
            if end_before_begin_obj.value > thresholds[constants.end_before_begin_max]:
                failing_metrics.append(end_before_begin_obj)

        for data_after_death_obj in self.data_after_death:
            if data_after_death_obj.value > thresholds[constants.data_after_death_max]:
                failing_metrics.append(data_after_death_obj)

        for route_obj in self.route_success:
            if route_obj.value < thresholds[constants.route_success_min]:
                failing_metrics.append(route_obj)

        for unit_obj in self.unit_success:
            if unit_obj.value < thresholds[constants.unit_success_min]:
                failing_metrics.append(unit_obj)

        for measurement_integration_obj in self.measurement_integration:
            if measurement_integration_obj.value < \
                    thresholds[constants.measurement_integration_min]:
                failing_metrics.append(measurement_integration_obj)

        for ingredient_integration_obj in self.ingredient_integration:
            if ingredient_integration_obj.value < \
                    thresholds[constants.route_success_min]:
                failing_metrics.append(ingredient_integration_obj)

        for date_datetime_disp_obj in self.date_datetime_disparity:
            if date_datetime_disp_obj.value > \
                    thresholds[constants.date_datetime_disparity_max]:
                failing_metrics.append(date_datetime_disp_obj)

        for erroneous_date_obj in self.erroneous_dates:
            if erroneous_date_obj.value > \
                    thresholds[constants.erroneous_dates_max]:
                failing_metrics.append(erroneous_date_obj)

        for person_id_failure_obj in self.person_id_failure_rate:
            if person_id_failure_obj.value > \
                    thresholds[constants.person_failure_rate_max]:
                failing_metrics.append(person_id_failure_obj)

        # NOTE: we are NOT reporting ACHILLES errors at the moment.
        # for achilles_error_obj in self.achilles_errors:
        #     if achilles_error_obj.value > \
        #             thresholds[constants.achilles_errors_max]:
        #         failing_metrics.append(achilles_error_obj)

        for visit_date_disparity_obj in self.visit_date_disparity:
            if visit_date_disparity_obj.value > \
                    thresholds[constants.visit_date_disparity_max]:
                failing_metrics.append(visit_date_disparity_obj)

        for visit_id_failure_obj in self.visit_id_failure:
            if visit_id_failure_obj.value > \
                    thresholds[constants.visit_id_failure_rate_max]:
                failing_metrics.append(visit_id_failure_obj)

        if not failing_metrics:  # no errors logged
            return None
        else:
            return failing_metrics
