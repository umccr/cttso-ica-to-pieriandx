#!/usr/bin/env python

"""
Match up the workflow output and working directories given a workflow id
"""

from typing import Dict, List, Optional
import json

from utils.logging import get_logger
from utils.globals import ICA_WES_MAX_PAGE_SIZE, \
    ICA_WES_CTTSO_RUN_NAME_REGEX, ICA_WES_CTTSO_RUN_NAME_REGEX_GROUPS
from libica.openapi import libwes
from libica.openapi.libwes import WorkflowRunCompact, WorkflowRun

logger = get_logger()


def get_ica_wes_configuration() -> libwes.Configuration:
    """
    Get the configuration object for ica wes
    :return:
    """
    from utils.ica_base import get_configuration
    return get_configuration(libwes.Configuration)


def get_all_workflow_runs() -> List[WorkflowRunCompact]:
    """
    Return all workflow runs
    :return:
    """
    workflow_runs_list: List[WorkflowRunCompact] = []
    next_page = True
    page_token = None
    configuration = get_ica_wes_configuration()

    with libwes.ApiClient(configuration) as api_client:
        # Create an instance of the API class
        api_instance = libwes.WorkflowRunsApi(api_client)

    while next_page:
        api_response: libwes.WorkflowRunList = api_instance.list_workflow_runs(
            status=["Succeeded"],
            page_size=ICA_WES_MAX_PAGE_SIZE,
            page_token=page_token
        )

        # Do we need to go another round?
        if api_response.next_page_token is None:
            next_page = False
        else:
            page_token = api_response.next_page_token

        # Extend the list
        workflow_runs_list.extend(api_response.items)

    return workflow_runs_list


def get_engine_parameters_from_workflow_run_obj(workflow_run_obj: WorkflowRun) -> Dict:
    """
    Get the engine parameters from a workflow id
    :param workflow_run_obj:
    :return:
    """
    if not hasattr(workflow_run_obj, "engine_parameters"):
        logger.error("Please ensure workflow run object has the engine parameters attribute before trying again")
    return json.loads(workflow_run_obj.engine_parameters)


def get_working_directory_from_workflow_run_obj(workflow_run_obj: WorkflowRun) -> str:
    """
    Get the gds working directory for a workflow id
    :param workflow_run_obj:
    :return:
    """
    return get_engine_parameters_from_workflow_run_obj(workflow_run_obj).get("workDirectory")


def get_output_directory_from_workflow_run_obj(workflow_run_obj: WorkflowRun) -> str:
    """
    Get the gds output directory for a workflow id
    :param workflow_run_obj:
    :return:
    """
    return get_engine_parameters_from_workflow_run_obj(workflow_run_obj).get("outputDirectory")


def get_ica_workflow_run_objs_from_library_names(libraries: List[str]) -> List[WorkflowRun]:
    """
    Get all workflow runs, then match from library names
    :return:
    """

    all_workflow_runs: List[WorkflowRunCompact] = get_all_workflow_runs()
    workflow_run_list = []

    for library in libraries:
        workflow_run_list.append(get_ica_workflow_run_obj_from_library_name(library,
                                                                            all_workflow_runs))

    return workflow_run_list


def get_ica_workflow_run_id_objs(workflow_run_ids: List[str]) -> List[WorkflowRun]:
    """
    Give a list of workflow run ids, return a list of workflow run objects
    :param workflow_run_ids: 
    :return: 
    """

    workflow_run_objs: List[WorkflowRun] = []

    with libwes.ApiClient(get_ica_wes_configuration()) as api_client:
        # Create an instance of the API class
        api_instance = libwes.WorkflowRunsApi(api_client)

    for workflow_run_id in workflow_run_ids:
        workflow_run_objs.append(api_instance.get_workflow_run(run_id=workflow_run_id,
                                                               include=["engineParameters"]))

    return workflow_run_objs


def get_ica_workflow_run_obj_from_library_name(library: str, all_workflow_runs: List[WorkflowRunCompact]) -> Optional[WorkflowRun]:
    """
    Traverse through workflow run list and return matching library
    :param library:
    :param all_workflow_runs:
    :return:
    """
    workflow_run_obj_list = list(filter(lambda x: ICA_WES_CTTSO_RUN_NAME_REGEX.match(x.name) is not None and
                                                  ICA_WES_CTTSO_RUN_NAME_REGEX.match(x.name).group(
                                                      ICA_WES_CTTSO_RUN_NAME_REGEX_GROUPS["library"]) == library,
                                        all_workflow_runs))

    # Check we found something
    if len(workflow_run_obj_list) == 0:
        logger.warning("Could not find ica workflow run for this library, skipping")
        return None

    # Otherwise append latest run (but recollect instance with engine parameters
    return get_ica_workflow_run_id_objs([workflow_run_obj_list[0].id])[0]



