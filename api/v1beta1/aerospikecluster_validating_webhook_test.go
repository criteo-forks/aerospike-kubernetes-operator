package v1beta1

import (
	"testing"
	"fmt"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var colorGreen = "\033[32m"
var colorRed = "\033[31m"
var colorReset = "\033[0m"

// To test: go to /api/v1beta1 and run this command:
// go test -run TestEnhancePesistentNamespaces
func TestEnhancePesistentNamespaces(t *testing.T) {
	TestAddPesistentNamespaceWithNotUsedDevices(t)
	TestAddPesistentNamespaceWithAlreadyUsedDevices(t)
	TestAddMultiplePesistentNamespacesWithNotUsedDevices(t)
	TestAddMultiplePesistentNamespacesWithAlreadyUsedDevices(t)
}

func TestAddPesistentNamespaceWithNotUsedDevices(t *testing.T) {
	oldNsConf := map[string][]string {
		"namespace-0": []string{"/opt/data01/xvd01", "/opt/data01/xvd02"},
	}
	newNsConf := map[string][]string {
		"namespace-0": []string{"/opt/data01/xvd01", "/opt/data01/xvd02"},
		"new-namespace": []string{"/opt/data02/xvd01", "/opt/data02/xvd02"},
	}

	aslog := logf.Log.WithName("Test validateNsConfUpdate")
	oldNamespaceConfig, newNamespaceConfig := prepareNsConfigurations(oldNsConf, newNsConf)

	err := validateNsConfUpdate(aslog, &newNamespaceConfig, &oldNamespaceConfig)
	if err != nil {
		fmt.Println(string(colorRed), "TestAddPesistentNamespaceWithNotUsedDevices failed.", string(colorReset))
		t.Fatalf("Got error while creating persistent namespace: %v", err)
	} else {
		fmt.Println(string(colorGreen), "TestAddPesistentNamespaceWithNotUsedDevices passed!", string(colorReset))
	}
}

// validateNsConfUpdate should return an error because we are using an already used device
func TestAddPesistentNamespaceWithAlreadyUsedDevices(t *testing.T) {
	oldNsConf := map[string][]string {
		"namespace-0": []string{"/opt/data01/xvd01", "/opt/data01/xvd02"},
	}
	newNsConf := map[string][]string {
		"namespace-0": []string{"/opt/data01/xvd01", "/opt/data01/xvd02"},
		"new-namespace": []string{"/opt/data01/xvd01", "/opt/data02/xvd02"}, // <- Using same device "/opt/data01/xvd01" as namespace-0
	}

	aslog := logf.Log.WithName("Test validateNsConfUpdate")
	oldNamespaceConfig, newNamespaceConfig := prepareNsConfigurations(oldNsConf, newNsConf)

	err := validateNsConfUpdate(aslog, &newNamespaceConfig, &oldNamespaceConfig)
	if err == nil {
		fmt.Println(string(colorRed), "TestAddPesistentNamespaceWithAlreadyUsedDevices failed.", string(colorReset))
		t.Fatalf("Got error while creating persistent namespace: %v", err)
	} else {
		fmt.Println(string(colorGreen), "TestAddPesistentNamespaceWithAlreadyUsedDevices passed!", string(colorReset))
	}
}

func TestAddMultiplePesistentNamespacesWithNotUsedDevices(t *testing.T) {
	oldNsConf := map[string][]string {
		"namespace-0": []string{"/opt/data01/xvd01", "/opt/data01/xvd02"},
	}
	newNsConf := map[string][]string {
		"namespace-0": []string{"/opt/data01/xvd01", "/opt/data01/xvd02"},
		"new-namespace-0": []string{"/opt/data02/xvd01", "/opt/data02/xvd02"},
		"new-namespace-1": []string{"/opt/data02/xvd03", "/opt/data02/xvd04"},
	}

	aslog := logf.Log.WithName("Test validateNsConfUpdate")
	oldNamespaceConfig, newNamespaceConfig := prepareNsConfigurations(oldNsConf, newNsConf)

	err := validateNsConfUpdate(aslog, &newNamespaceConfig, &oldNamespaceConfig)
	if err != nil {
		fmt.Println(string(colorRed), "TestAddMultiplePesistentNamespacesWithNotUsedDevices failed.", string(colorReset))
		t.Fatalf("Got error while creating persistent namespace: %v", err)
	} else {
		fmt.Println(string(colorGreen), "TestAddMultiplePesistentNamespacesWithNotUsedDevices passed!", string(colorReset))
	}
}

// validateNsConfUpdate should return an error because we are using an already used device
func TestAddMultiplePesistentNamespacesWithAlreadyUsedDevices(t *testing.T) {
	oldNsConf := map[string][]string {
		"namespace-0": []string{"/opt/data01/xvd01", "/opt/data01/xvd02"},
	}
	newNsConf := map[string][]string {
		"namespace-0": []string{"/opt/data01/xvd01", "/opt/data01/xvd02"},
		"new-namespace-0": []string{"/opt/data02/xvd01", "/opt/data02/xvd02"},
		"new-namespace-1": []string{"/opt/data02/xvd01", "/opt/data02/xvd04"}, // <- Using same device "/opt/data02/xvd01" as new-namespace-0
	}

	aslog := logf.Log.WithName("Test validateNsConfUpdate")
	oldNamespaceConfig, newNamespaceConfig := prepareNsConfigurations(oldNsConf, newNsConf)

	err := validateNsConfUpdate(aslog, &newNamespaceConfig, &oldNamespaceConfig)
	if err == nil {
		fmt.Println(string(colorRed), "TestAddMultiplePesistentNamespacesWithAlreadyUsedDevices failed.", string(colorReset))
		t.Fatalf("Got error while creating persistent namespace: %v", err)
	} else {
		fmt.Println(string(colorGreen), "TestAddMultiplePesistentNamespacesWithAlreadyUsedDevices passed!", string(colorReset))
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

func prepareNsConfigurations(oldNamespaceConf map[string][]string, newNamespaceConf map[string][]string) (AerospikeConfigSpec, AerospikeConfigSpec) {
	oldNamespaceConfig := buildEmptyNsConf()
	newNamespaceConfig := buildEmptyNsConf()

	for namespace, deviceList := range oldNamespaceConf {
		for _, device := range deviceList{
			addDeviceToPersistentNamesapce(namespace, device, &oldNamespaceConfig)
		}
	}

	for namespace, deviceList := range newNamespaceConf {
		for _, device := range deviceList{
			addDeviceToPersistentNamesapce(namespace, device, &newNamespaceConfig)
		}
	}

	return oldNamespaceConfig, newNamespaceConfig
}
