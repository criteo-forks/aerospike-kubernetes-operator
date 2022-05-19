package v1beta1

import (
	"fmt"
	"testing"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var aslog = logf.Log.WithName("Test validateNsConfUpdate")

func TestAddPesistentNamespaceWithNotUsedDevices(t *testing.T) {
	oldNsConf := map[string][]string{
		"namespace-0": {"/dev/nvme1", "/dev/nvme2"},
	}
	newNsConf := map[string][]string{
		"namespace-0":   {"/dev/nvme1", "/dev/nvme2"},
		"new-namespace": {"/dev/nvme3", "/dev/nvme4"},
	}

	oldNamespaceConfig, newNamespaceConfig := prepareNsConfigurations(oldNsConf, newNsConf)

	err := validateNsConfUpdate(aslog, &newNamespaceConfig, &oldNamespaceConfig)
	if err != nil {
		t.Fatalf(
			"Adding a new namespace with unused devices.\nExpected: nil.\nTest output: %v", err,
		)
	}
}

func TestAddPesistentNamespaceWithAlreadyUsedDevices(t *testing.T) {
	device, previousNamespace, namespace := "/dev/nvme1", "namespace-0", "new-namespace"
	expectedResult := fmt.Sprintf(
		"device %s is already being referenced in multiple namespaces (%s, %s)",
		device, previousNamespace, namespace,
	)

	oldNsConf := map[string][]string{
		"namespace-0": {"/dev/nvme1", "/dev/nvme2"},
	}
	newNsConf := map[string][]string{
		"namespace-0":   {"/dev/nvme1", "/dev/nvme2"},
		"new-namespace": {"/dev/nvme1", "/dev/nvme4"}, // <- Using same device "/dev/nvme1" as namespace-0
	}

	oldNamespaceConfig, newNamespaceConfig := prepareNsConfigurations(oldNsConf, newNsConf)

	err := validateNsConfUpdate(aslog, &newNamespaceConfig, &oldNamespaceConfig)
	if err == nil {
		t.Fatalf(
			"Adding namespace with already used devices.\nExpected: %s.\nTest output: %v",
			expectedResult, err,
		)
	}
}

func TestAddMultiplePesistentNamespacesWithNotUsedDevices(t *testing.T) {
	oldNsConf := map[string][]string{
		"namespace-0": {"/dev/nvme1", "/dev/nvme2"},
	}
	newNsConf := map[string][]string{
		"namespace-0":     {"/dev/nvme1", "/dev/nvme2"},
		"new-namespace-0": {"/dev/nvme3", "/dev/nvme4"},
		"new-namespace-1": {"/dev/nvme5", "/dev/nvme6"},
	}

	oldNamespaceConfig, newNamespaceConfig := prepareNsConfigurations(oldNsConf, newNsConf)

	err := validateNsConfUpdate(aslog, &newNamespaceConfig, &oldNamespaceConfig)
	if err != nil {
		t.Fatalf(
			"Adding multiple namespaces with unused devices.\nExpected: nil.\nTest output: %v", err,
		)
	}
}

func TestAddMultiplePesistentNamespacesWithAlreadyUsedDevices(t *testing.T) {
	device, previousNamespace, namespace := "/dev/nvme3", "new-namespace-0", "new-namespace-1"
	expectedResult := fmt.Sprintf(
		"device %s is already being referenced in multiple namespaces (%s, %s)",
		device, previousNamespace, namespace,
	)

	oldNsConf := map[string][]string{
		"namespace-0": {"/dev/nvme1", "/dev/nvme2"},
	}
	newNsConf := map[string][]string{
		"namespace-0":     {"/dev/nvme1", "/dev/nvme2"},
		"new-namespace-0": {"/dev/nvme3", "/dev/nvme4"},
		"new-namespace-1": {"/dev/nvme3", "/dev/nvme6"}, // <- Using same device "/dev/nvme3" as new-namespace-0
	}

	oldNamespaceConfig, newNamespaceConfig := prepareNsConfigurations(oldNsConf, newNsConf)

	err := validateNsConfUpdate(aslog, &newNamespaceConfig, &oldNamespaceConfig)
	if err == nil {
		t.Fatalf(
			"Adding multiple namespaces with already used devices.\nExpected: %s.\nTest output: %v",
			expectedResult, err,
		)
	}
}

func TestAddDevicesToExistantNamespace(t *testing.T) {
	oldNsConf := map[string][]string{
		"namespace-0":     {"/dev/nvme1", "/dev/nvme2"},
		"new-namespace-0": {"/dev/nvme3", "/dev/nvme4"},
	}
	newNsConf := map[string][]string{
		"namespace-0": {"/dev/nvme1", "/dev/nvme2"},
		"new-namespace-0": {
			"/dev/nvme3", "/dev/nvme4",
			"/dev/nvme5", "/dev/nvme6",
		},
	}

	oldNamespaceConfig, newNamespaceConfig := prepareNsConfigurations(oldNsConf, newNsConf)

	err := validateNsConfUpdate(aslog, &newNamespaceConfig, &oldNamespaceConfig)
	if err != nil {
		t.Fatalf(
			"Adding unused devices to an existing namespace.\nExpected: nil.\nTest output: %v", err,
		)
	}
}

func TestAddUsedDeviceByAnotherNamespaceToExistantNamespace(t *testing.T) {
	device, previousNamespace, namespace := "/dev/nvme1", "namespace-0", "new-namespace-0"
	expectedResult := fmt.Sprintf(
		"device %s is already being referenced in multiple namespaces (%s, %s)",
		device, previousNamespace, namespace,
	)

	oldNsConf := map[string][]string{
		"namespace-0":     {"/dev/nvme1", "/dev/nvme2"},
		"new-namespace-0": {"/dev/nvme3", "/dev/nvme4"},
	}
	newNsConf := map[string][]string{
		"namespace-0": {"/dev/nvme1", "/dev/nvme2"},
		"new-namespace-0": {
			"/dev/nvme3", "/dev/nvme4",
			"/dev/nvme1", "/dev/nvme6", // <- Using same device "/dev/nvme1" as namespace-0
		},
	}

	oldNamespaceConfig, newNamespaceConfig := prepareNsConfigurations(oldNsConf, newNsConf)

	err := validateNsConfUpdate(aslog, &newNamespaceConfig, &oldNamespaceConfig)
	if err == nil {
		t.Fatalf(
			"Adding used devices by another namespace to an existing namespace.\nExpected: %s.\nTest output: %v",
			expectedResult, err,
		)
	}
}

func TestAddUsedDeviceBySameNamespaceToExistantNamespace(t *testing.T) {
	device, previousNamespace, namespace := "/dev/nvme3", "new-namespace-0", "new-namespace-0"
	expectedResult := fmt.Sprintf(
		"device %s is already being referenced in multiple namespaces (%s, %s)",
		device, previousNamespace, namespace,
	)

	oldNsConf := map[string][]string{
		"namespace-0":     {"/dev/nvme1", "/dev/nvme2"},
		"new-namespace-0": {"/dev/nvme3", "/dev/nvme4"},
	}
	newNsConf := map[string][]string{
		"namespace-0": {"/dev/nvme1", "/dev/nvme2"},
		"new-namespace-0": {
			"/dev/nvme3", "/dev/nvme4",
			"/dev/nvme3", "/dev/nvme6", // <- Using same device "/dev/nvme3" as new-namespace-0
		},
	}

	oldNamespaceConfig, newNamespaceConfig := prepareNsConfigurations(oldNsConf, newNsConf)

	err := validateNsConfUpdate(aslog, &newNamespaceConfig, &oldNamespaceConfig)
	if err == nil {
		t.Fatalf(
			"Adding used devices by same namespace to an existing namespace.\nExpected: %s.\nTest output: %v",
			expectedResult, err,
		)
	}
}

func TestAddDevicesToMultipleExistantNamespaces(t *testing.T) {
	oldNsConf := map[string][]string{
		"namespace-0":     {"/dev/nvme1", "/dev/nvme2"},
		"new-namespace-0": {"/dev/nvme3", "/dev/nvme4"},
		"new-namespace-1": {"/dev/nvme7", "/dev/nvme8"},
	}
	newNsConf := map[string][]string{
		"namespace-0": {"/dev/nvme1", "/dev/nvme2"},
		"new-namespace-0": {
			"/dev/nvme3", "/dev/nvme4",
			"/dev/nvme5", "/dev/nvme6",
		},
		"new-namespace-1": {
			"/dev/nvme7", "/dev/nvme8",
			"/dev/nvme9", "/dev/nvme10",
		},
	}

	oldNamespaceConfig, newNamespaceConfig := prepareNsConfigurations(oldNsConf, newNsConf)

	err := validateNsConfUpdate(aslog, &newNamespaceConfig, &oldNamespaceConfig)
	if err != nil {
		t.Fatalf(
			"Adding unused devices to an existing namespaces.\nExpected: nil.\nTest output: %v",
			err,
		)
	}
}

func TestAddUsedDeviceToMultipleExistantNamespace(t *testing.T) {
	device, previousNamespace, namespace := "/dev/nvme5", "new-namespace-0", "new-namespace-1"
	expectedResult := fmt.Sprintf(
		"device %s is already being referenced in multiple namespaces (%s, %s)",
		device, previousNamespace, namespace,
	)

	oldNsConf := map[string][]string{
		"namespace-0":     {"/dev/nvme1", "/dev/nvme2"},
		"new-namespace-0": {"/dev/nvme3", "/dev/nvme4"},
		"new-namespace-1": {"/dev/nvme7", "/dev/nvme8"},
	}
	newNsConf := map[string][]string{
		"namespace-0": {"/dev/nvme1", "/dev/nvme2"},
		"new-namespace-0": {
			"/dev/nvme3", "/dev/nvme4",
			"/dev/nvme5", "/dev/nvme6",
		},
		"new-namespace-1": {
			"/dev/nvme7", "/dev/nvme8",
			"/dev/nvme5", "/dev/nvme10", // <- Using same device "/dev/nvme5" as new-namespace-0
		},
	}

	oldNamespaceConfig, newNamespaceConfig := prepareNsConfigurations(oldNsConf, newNsConf)

	err := validateNsConfUpdate(aslog, &newNamespaceConfig, &oldNamespaceConfig)
	if err == nil {
		t.Fatalf(
			"Adding used devices to an existing namespace.\nExpected: %s.\nTest output: %v",
			expectedResult, err,
		)
	}
}

func TestUseDeletedDevice(t *testing.T) {
	device, oldNamespace, newNamespace := "/dev/nvme1", "namespace-0", "new-namespace-0"
	expectedResult := fmt.Sprintf(
		"device %s is being reallocated from namespace %s to namespace %s without being cleaned-up first",
		device, oldNamespace, newNamespace,
	)

	oldNsConf := map[string][]string{
		"namespace-0":     {"/dev/nvme1", "/dev/nvme2"},
		"new-namespace-0": {"/dev/nvme3", "/dev/nvme4"},
	}
	newNsConf := map[string][]string{
		"namespace-0": {"/dev/nvme2"},
		"new-namespace-0": {
			"/dev/nvme3", "/dev/nvme4", "/dev/nvme1", // <- Using deleted device "/dev/nvme1"
		},
	}

	oldNamespaceConfig, newNamespaceConfig := prepareNsConfigurations(oldNsConf, newNsConf)

	err := validateNsConfUpdate(aslog, &newNamespaceConfig, &oldNamespaceConfig)
	if err == nil {
		t.Fatalf(
			"Adding devices deleted from an existing namespace.\nExpected: %s.\nTest output: %v",
			expectedResult, err,
		)
	}
}

func TestDeleteDeviceFromNamespace(t *testing.T) {
	device, oldNamespace := "/dev/nvme1", "namespace-0"
	expectedResult := fmt.Sprintf(
		"device %s is being removed from namespace %s. Operation not yet supported by the operator",
		device, oldNamespace,
	)

	oldNsConf := map[string][]string{
		"namespace-0":     {"/dev/nvme1", "/dev/nvme2"},
		"new-namespace-0": {"/dev/nvme3", "/dev/nvme4"},
	}
	newNsConf := map[string][]string{
		"namespace-0":     {"/dev/nvme2"},
		"new-namespace-0": {"/dev/nvme3", "/dev/nvme4"},
	}

	oldNamespaceConfig, newNamespaceConfig := prepareNsConfigurations(oldNsConf, newNsConf)

	err := validateNsConfUpdate(aslog, &newNamespaceConfig, &oldNamespaceConfig)
	if err == nil {
		t.Fatalf(
			"Deleting device from a namespace.\nExpected: %s.\nTest output: %v",
			expectedResult, err,
		)
	}
}

func TestChangeStorageEngineType(t *testing.T) {
	key, namespace := "type", "namespace-0"
	expectedResult := fmt.Sprintf(
		"%s of storage-engine cannot be changed (namespace=%s)", key, namespace,
	)

	oldNsConf := map[string][]string{
		"namespace-0":     {"/dev/nvme1", "/dev/nvme2"},
		"new-namespace-0": {"/dev/nvme3", "/dev/nvme4"},
	}
	newNsConf := map[string][]string{
		"namespace-0":     {},
		"new-namespace-0": {"/dev/nvme3", "/dev/nvme4"},
	}

	oldNamespaceConfig, newNamespaceConfig := prepareNsConfigurations(oldNsConf, newNsConf)

	err := validateNsConfUpdate(aslog, &newNamespaceConfig, &oldNamespaceConfig)
	if err == nil {
		t.Fatalf(
			"Changing type of storage-engine for a namespace.\nExpected: %s.\nTest output: %v",
			expectedResult, err,
		)
	}
}

//******************************************************************************
// 									Helper
//******************************************************************************

func buildEmptyNsConf() AerospikeConfigSpec {
	emptyConf := AerospikeConfigSpec{
		Value: map[string]interface{}{
			"namespaces":              []interface{}{},
			"tls-name":                "test-tls",
			"replication-factor":      2,
			"tls-authenticate-client": "test-auth-tls",
		},
	}
	return emptyConf
}

func addDeviceToPersistentNamesapce(namespace string, device string, conf *AerospikeConfigSpec) {
	config := conf.Value
	nsConfList := config["namespaces"].([]interface{})

	exists, index := namespaceAlreadyExists(nsConfList, namespace)

	if !exists {
		newNamespace := map[string]interface{}{
			"name": namespace,
			"storage-engine": map[string]interface{}{
				"type":    "device",
				"devices": []string{device},
			},
		}
		nsConfList = append(nsConfList, newNamespace)
	} else {
		nsConf := nsConfList[index].(map[string]interface{})
		storage := nsConf["storage-engine"].(map[string]interface{})
		devices := storage["devices"].([]string)
		devices = append(devices, device)
		storage["devices"] = devices
		nsConf["storage-engine"] = storage
		nsConfList[index] = nsConf
	}

	conf.Value["namespaces"] = nsConfList
}

func namespaceAlreadyExists(nsConfList []interface{}, namespace string) (bool, int) {
	if len(nsConfList) == 0 {
		return false, -1
	}

	for index, nsConfInterface := range nsConfList {
		ncConf := nsConfInterface.(map[string]interface{})
		if namespace == ncConf["name"] {
			return true, index
		}
	}

	return false, -1
}

func addInMemoryNamespace(namespace string, conf *AerospikeConfigSpec) {
	config := conf.Value
	nsConfList := config["namespaces"].([]interface{})

	newNamespace := map[string]interface{}{
		"name": namespace,
		"storage-engine": map[string]interface{}{
			"type":    "memory",
			"devices": []string{},
		},
	}
	nsConfList = append(nsConfList, newNamespace)

	conf.Value["namespaces"] = nsConfList
}

func prepareNsConfigurations(oldNamespaceConf map[string][]string, newNamespaceConf map[string][]string) (AerospikeConfigSpec, AerospikeConfigSpec) {
	oldNamespaceConfig := buildEmptyNsConf()
	newNamespaceConfig := buildEmptyNsConf()

	for namespace, deviceList := range oldNamespaceConf {
		if len(deviceList) == 0 {
			addInMemoryNamespace(namespace, &oldNamespaceConfig)
		} else {
			for _, device := range deviceList {
				addDeviceToPersistentNamesapce(namespace, device, &oldNamespaceConfig)
			}
		}
	}

	for namespace, deviceList := range newNamespaceConf {
		if len(deviceList) == 0 {
			addInMemoryNamespace(namespace, &oldNamespaceConfig)
		} else {
			for _, device := range deviceList {
				addDeviceToPersistentNamesapce(namespace, device, &newNamespaceConfig)
			}
		}
	}

	return oldNamespaceConfig, newNamespaceConfig
}
