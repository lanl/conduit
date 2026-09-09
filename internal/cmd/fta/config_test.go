// Copyright 2026. Triad National Security, LLC. All rights reserved.

package ftacmd

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/lanl/conduit/internal/fta"
	"github.com/lanl/conduit/internal/fta/plugin"
	"github.com/lanl/conduit/internal/fta/plugins/pftool"
	"github.com/spf13/viper"
)

func TestPluginConfigsPreserveDefaults(t *testing.T) {
	for pluginKey, p := range fta.PluginMap {
		defaultConfig := p.GetDefaultConfig()
		if defaultConfig == nil {
			continue
		}

		for name, partial := range partialConfigs(defaultConfig) {
			t.Run(pluginKey+"/"+name, func(t *testing.T) {
				viper.Reset()

				config := reflect.New(reflect.TypeOf(defaultConfig))
				config.Elem().Set(reflect.ValueOf(defaultConfig))

				viper.Set("plugins."+pluginKey, partial)

				if err := plugin.GetPluginConfigsFromViper(pluginKey, config.Interface()); err != nil {
					t.Fatal(err)
				}

				if !reflect.DeepEqual(config.Elem().Interface(), defaultConfig) {
					t.Errorf("defaults not preserved:\ngot:  %#v\nwant: %#v", config.Elem().Interface(), defaultConfig)
				}
			})
		}
	}
}

func partialConfigs(config any) map[string]map[string]any {
	v := reflect.ValueOf(config)
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		panic(fmt.Sprintf("plugin default config must be a struct, got %s", v.Kind()))
	}

	typ := v.Type()

	full := make(map[string]any)
	keys := make([]string, 0, v.NumField())

	for i := 0; i < v.NumField(); i++ {
		field := typ.Field(i)

		if !field.IsExported() {
			continue
		}

		key := field.Tag.Get("mapstructure")
		if key == "-" {
			continue
		}

		if idx := strings.IndexByte(key, ','); idx >= 0 {
			key = key[:idx]
		}

		if key == "" {
			key = strings.ToLower(field.Name)
		}

		full[key] = v.Field(i).Interface()
		keys = append(keys, key)
	}

	result := make(map[string]map[string]any)

	for _, omitted := range keys {
		partial := make(map[string]any)

		for key, value := range full {
			if key != omitted {
				partial[key] = value
			}
		}

		result["omitted-"+omitted] = partial
	}

	return result
}

func TestPluginConfigsUseDefaultsWhenAbsent(t *testing.T) {
	for pluginKey, p := range fta.PluginMap {
		defaultConfig := p.GetDefaultConfig()
		if defaultConfig == nil {
			continue
		}

		t.Run(pluginKey, func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)

			setPluginDefaults()

			config := reflect.New(reflect.TypeOf(defaultConfig))

			if err := plugin.GetPluginConfigsFromViper(pluginKey, config.Interface()); err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(config.Elem().Interface(), defaultConfig) {
				t.Errorf("default config not applied:\ngot:  %#v\nwant: %#v", config.Elem().Interface(), defaultConfig)
			}
		})
	}
}

func TestPluginConfigsOverrideDefaults(t *testing.T) {
	for pluginKey, p := range fta.PluginMap {
		defaultConfig := p.GetDefaultConfig()
		if defaultConfig == nil {
			continue
		}

		v := reflect.ValueOf(defaultConfig)
		if v.Kind() == reflect.Pointer {
			v = v.Elem()
		}

		typ := v.Type()

		for i := 0; i < v.NumField(); i++ {
			field := typ.Field(i)
			if !field.IsExported() {
				continue
			}

			key := field.Tag.Get("mapstructure")
			if key == "-" {
				continue
			}
			if idx := strings.IndexByte(key, ','); idx >= 0 {
				key = key[:idx]
			}
			if key == "" {
				key = strings.ToLower(field.Name)
			}

			override, ok := testOverrideValue(v.Field(i))
			if !ok {
				continue
			}

			t.Run(pluginKey+"/"+key, func(t *testing.T) {
				viper.Reset()

				config := reflect.New(reflect.TypeOf(defaultConfig))
				config.Elem().Set(reflect.ValueOf(defaultConfig))

				viper.Set("plugins."+pluginKey, map[string]any{
					key: override,
				})

				if err := plugin.GetPluginConfigsFromViper(pluginKey, config.Interface()); err != nil {
					t.Fatal(err)
				}

				expected := reflect.New(reflect.TypeOf(defaultConfig)).Elem()
				expected.Set(reflect.ValueOf(defaultConfig))
				expected.Field(i).Set(reflect.ValueOf(override))

				if !reflect.DeepEqual(config.Elem().Interface(), expected.Interface()) {
					t.Errorf(
						"config overlay incorrect:\ngot:  %#v\nwant: %#v",
						config.Elem().Interface(),
						expected.Interface(),
					)
				}
			})
		}
	}
}

func testOverrideValue(v reflect.Value) (any, bool) {
	override := reflect.New(v.Type()).Elem()

	switch v.Kind() {
	case reflect.String:
		override.SetString(v.String() + "-test-override")

	case reflect.Bool:
		override.SetBool(!v.Bool())

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		override.SetInt(v.Int() + 1)

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		override.SetUint(v.Uint() + 1)

	case reflect.Float32, reflect.Float64:
		override.SetFloat(v.Float() + 1)

	case reflect.Slice:
		if v.Type().Elem().Kind() != reflect.String {
			return nil, false
		}

		override = reflect.MakeSlice(v.Type(), 1, 1)
		override.Index(0).SetString("test-override")

	default:
		return nil, false
	}

	return override.Interface(), true
}

func TestPftoolAllowsEmptyArgs(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	config := pftool.DefaultPftoolPluginConfig()

	viper.Set("plugins."+pftool.PftoolPluginKey, map[string]any{
		"pfcp-args": []string{},
	})

	if err := plugin.GetPluginConfigsFromViper(pftool.PftoolPluginKey, &config); err != nil {
		t.Fatal(err)
	}

	if len(config.PfcpArgs) != 0 {
		t.Errorf("expected empty pfcp args, got %v", config.PfcpArgs)
	}
}
