#!/usr/bin/env bash
#
# Copyright 2022 Authors of Global Resource Service.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


### Only support gcloud 
### Please ensure gcloud is installed before run this script
GRS_ROOT=$(dirname "${BASH_SOURCE[0]}")/..

source "${GRS_ROOT}/hack/test-config.sh"

echo "Tear down server, region simulator and clients... "

function delete-image {
        local image_name="$1"
        if gcloud compute images describe "${image_name}" --project "${PROJECT}" &>/dev/null; then
                echo "Deleting existing template image: ${image_name}"
                gcloud compute images delete \
                        "${image_name}" \
                        --project "${PROJECT}" \
                        --quiet 
        fi
}

function delete-instance-template {
        local template_name="$1"
        if gcloud compute instance-templates describe "${template_name}" --project "${PROJECT}" &>/dev/null; then
                echo "Try to Delete existing instance template: ${template_name}, if failed, Please manully delete instance group first!"
                gcloud compute instance-templates delete \
                        "${template_name}" \
                        --project "${PROJECT}" \
                        --quiet 
        fi
}

function delete-instance-groups {
        local group_name="$1"
        local zone="$2"
        if gcloud compute instance-groups managed describe "${group_name}" --project "${PROJECT}" --zone "${zone}" &>/dev/null; then
                 echo "Try to Delete existing instance groups: ${group_name}"
                 gcloud compute instance-groups managed delete \
                         "${group_name}" \
                         --project "${PROJECT}" \
                         --zone "${zone}" \
                         --quiet 
        fi
}

function delete-vm-instance {
        local vm_name="$1"
        local instance_zone="$2"
        if gcloud compute instances describe "${vm_name}" --project "${PROJECT}" --zone "${instance_zone}" &>/dev/null; then
                echo "Try to delete existing instance : ${vm_name}"
                gcloud compute instances delete \
                        "${vm_name}" \
                        --project "${PROJECT}" \
                        --zone "${instance_zone}"  \
                        --quiet 
        fi
}    

function delete-machine-image {
        local image_name="$1"
        if gcloud compute machine-images describe "${image_name}" --project "${PROJECT}" &>/dev/null; then
                echo "Deleting existing machine image: ${image_name}"
                gcloud compute machine-images delete "${image_name}" \
                        --project "${PROJECT}" \
                        --quiet
        fi
}

###############
#   main function
###############

if [[ "${SIM_AUTO_DELETE}" == "true" && ${SIM_NUM} -gt 0 ]]; then
        echo "Deleting simulator resources"
        IFS=','; INSTANCE_SIM_ZONE=($SIM_ZONE); unset IFS;
        if [ ${#INSTANCE_SIM_ZONE[@]} == 1 ]; then
                delete-instance-groups "${SIM_INSTANCE_PREFIX}-${INSTANCE_SIM_ZONE[0]}-mig" "${INSTANCE_SIM_ZONE[0]}"
                delete-instance-template "${SIM_INSTANCE_PREFIX}-template"
        else
                index=0
                for zone in "${INSTANCE_SIM_ZONE[@]}"; do
                        delete-vm-instance "${SIM_INSTANCE_PREFIX}-${zone}-${index}" "${zone}" &
                        index=$(($index + 1)) 
                done
        fi
        if [ "${SIMIMAGE_AUTO_DELETE}" == "true" ]; then
                #waiting 60 seconds to get all instances deleted before delete images
                sleep 60
                delete-image "${SIM_IMAGE_NAME}"
                delete-machine-image  "${SIM_IMAGE_NAME}"
        fi
fi

if [[ "${CLIENT_AUTO_DELETE}" == "true"  && ${CLIENT_NUM} -gt 0 ]]; then
        echo "Deleting client scheduler resources"
        IFS=','; INSTANCE_CLIENT_ZONE=($CLIENT_ZONE); unset IFS;
        if [ ${#INSTANCE_CLIENT_ZONE[@]} == 1 ]; then
                delete-instance-groups "${CLIENT_INSTANCE_PREFIX}-${INSTANCE_CLIENT_ZONE[0]}-mig" "${INSTANCE_CLIENT_ZONE[0]}"
                delete-instance-template "${CLIENT_INSTANCE_PREFIX}-template"
        else
                index=0
                for zone in "${INSTANCE_CLIENT_ZONE[@]}"; do
                        delete-vm-instance "${CLIENT_INSTANCE_PREFIX}-${zone}-${index}" "${zone}" &
                        index=$(($index + 1)) 
                done
        fi
        if [ "${CLIENTIMAGE_AUTO_DELETE}" == "true" ]; then
                #waiting 60 seconds to get all instances deleted before delete images
                sleep 60
                delete-image "${CLIENT_IMAGE_NAME}"
                delete-machine-image  "${CLIENT_IMAGE_NAME}"
        fi
fi

if [[ "${SERVER_AUTO_DELETE}" == "true"  && ${SERVER_NUM} -gt 0 ]]; then
        echo "Deleting server resources"
        IFS=','; INSTANCE_SERVER_ZONE=($SERVER_ZONE); unset IFS;
        if [ ${#INSTANCE_SERVER_ZONE[@]} == 1 ]; then
                delete-instance-groups "${SERVER_INSTANCE_PREFIX}-${INSTANCE_SERVER_ZONE[0]}-mig" "${INSTANCE_SERVER_ZONE[0]}"
                delete-instance-template "${SERVER_INSTANCE_PREFIX}-template"
        else
                index=0
                for zone in "${INSTANCE_SERVER_ZONE[@]}"; do
                        delete-vm-instance "${SERVER_INSTANCE_PREFIX}-${zone}-${index}" "${zone}" &
                        index=$(($index + 1)) 
                done
        fi
        if [ "${SERVERIMAGE_AUTO_DELETE}" == "true" ]; then
                #waiting 60 seconds to get all instances deleted before delete images
                sleep 60
                delete-image "${SERVER_IMAGE_NAME}"
                delete-machine-image  "${SERVER_IMAGE_NAME}"
        fi
fi

if [[ "${ADMINCLIENT_AUTO_DELETE}" == "true"  && ${ADMINCLIENT_NUM} -gt 0 ]]; then
        echo "Deleting admin client resources"
        IFS=','; INSTANCE_ADMINCLIENT_ZONE=($ADMINCLIENT_ZONE); unset IFS;
        index=0
        for zone in "${INSTANCE_ADMINCLIENT_ZONE[@]}"; do
                delete-vm-instance "${ADMINCLIENT_INSTANCE_PREFIX}-${zone}-${index}" "${zone}" &
                index=$(($index + 1)) 
        done
        if [ "${ADMINCLIENTIMAGE_AUTO_DELETE}" == "true" ]; then
                #waiting 60 seconds to get all instances deleted before delete images
                sleep 60
                delete-machine-image  "${ADMINCLIENT_IMAGE_NAME}"
        fi
fi

echo "Done. All resources deleted successfully"

