# 55-swift-sproxyd.sh - Devstack extras script to configure s-object with swiftsproxyd driver

function install_sproxyd_driver {
    # Get the correct sproxyd-client version from the requirements
    local scal_sproxyd_client=$(grep 'scality-sproxyd-client' ${WORKSPACE}/requirements.txt)
    if [[ -n "$scal_sproxyd_client" ]]; then
        sudo pip install "$scal_sproxyd_client"
    fi
    # For some reason, doing this failed: keystone would not work,
    # complaining about "ArgsAlreadyParsedError: arguments already parsed:".
    #sudo python setup.py install
    sudo pip install .
}

# Shameless ripoff of devstack/lib/swift
function enable_sproxyd_driver {
    for node_number in ${SWIFT_REPLICAS_SEQ}; do
        local swift_node_config=${SWIFT_CONF_DIR}/object-server/${node_number}.conf
        iniset ${swift_node_config} app:object-server use egg:swift_scality_backend#sproxyd_object
        # Host and port need to be configurable
        iniset ${swift_node_config} app:object-server sproxyd_host 127.0.0.1:81
        # /proxy_path need to be configurable
        iniset ${swift_node_config} app:object-server sproxyd_path /proxy/chord_path
        # splice need to be configurable
        iniset ${swift_node_config} app:object-server splice yes
    done
}

function configure_swift_functional_tests {
    if [[ $DEVSTACK_BRANCH == "stable/kilo" ]]; then
        # keystone V3 support in devstack is not working properly
        testfile=${SWIFT_CONF_DIR}/test.conf
        iniset ${testfile} func_test auth_version 2
        iniset ${testfile} func_test auth_prefix /v2.0/
        # Disable temporary url feature so that related tests gets skipped
        # Some of those are failing during setup phase with credentials related errors
        # So most probably a keystone API version related issue
        iniset /etc/swift/proxy-server.conf DEFAULT disallowed_sections tempurl
    fi
}

if is_service_enabled s-object; then
    if [[ "$1" == "stack" && "$2" == "install" ]]; then
        echo_summary "Post-config hook : install swift-sproxyd."
        install_sproxyd_driver
        configure_swift_functional_tests
    fi
    if [[ "$1" == "stack" && "$2" == "post-config" ]]; then
        echo_summary "Post-config hook : enable swift-sproxyd."
        enable_sproxyd_driver
    fi
fi
