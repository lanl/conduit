// Copyright 2026. Triad National Security, LLC. All rights reserved.

package clicmd

import (
	"fmt"
	"os"

	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	"github.com/lanl/conduit/internal/cli/client"
	"github.com/lanl/conduit/internal/cli/util"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/encoding/protojson"
	"encoding/json"
	"github.com/buger/goterm"
)

var (
	serverCmdVerbose bool
	serverCmdJSON    bool
)

var serverCmd = &cobra.Command{
	Use:    "server",
	Short:  "control the conduit server state",
	Long:   `This subcommand allows an admin to control the conduit server state`,
	Hidden: true,
}

var serverStartCmd = &cobra.Command{
	Use:    "start",
	Short:  "starts the conduit server",
	Long:   `This subcommand starts the conduit server so it will start progressing transfers again`,
	Hidden: false,
	Run: func(cmd *cobra.Command, args []string) {
		logger := logrus.New()
		if debug {
			logger.SetLevel(logrus.DebugLevel)
			logger.Debugf("loaded cli config from: %v", viper.ConfigFileUsed())
		}

		clientCertKeyBundle, err := cmd.Flags().GetString("cert-key-bundle")
		if err != nil {
			fmt.Printf("failed to get cert-key-bundle flag: %v\n", err)
			os.Exit(1)
		}
		clientCert, clientKey, err := util.GetUserCertAndKey(viper.GetString(defaults.ConfigClientCertKey), viper.GetString(defaults.ConfigClientKeyKey), clientCertKeyBundle, defaults.DefaultBundlePath)
		if err != nil {
			fmt.Printf("failed to get client cert and key: %v\n", err)
			os.Exit(1)
		}
		logger.Debugf("using user cert [%v] and key [%v]", clientCert, clientKey)

		client, err := client.NewClient(logger, quiet, clientCert, clientKey)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		resp, err := client.ServerControl(proto.ServerControlAction_SERVER_CONTROL_START)
		if err != nil {
			fmt.Printf("server start request failed: %v\n", err)
			os.Exit(1)
		} else {
			fmt.Printf("successfully started conduit server. current state: %s\n", resp.GetServerState())
		}
	},
}

var serverDrainCmd = &cobra.Command{
	Use:    "drain",
	Short:  "drains the conduit server",
	Long:   `This subcommand drains the conduit server so it will progress currently queued transfers but will not progress any that are newly submitted. Note that this only works if all conduit instances for a cluster are draining. Use 'stop' to safely stop a single conduit instance`,
	Hidden: false,
	Run: func(cmd *cobra.Command, args []string) {
		logger := logrus.New()
		if debug {
			logger.SetLevel(logrus.DebugLevel)
		}

		clientCertKeyBundle, err := cmd.Flags().GetString("cert-key-bundle")
		if err != nil {
			fmt.Printf("failed to get cert-key-bundle flag: %v\n", err)
			os.Exit(1)
		}
		clientCert, clientKey, err := util.GetUserCertAndKey(viper.GetString(defaults.ConfigClientCertKey), viper.GetString(defaults.ConfigClientKeyKey), clientCertKeyBundle, defaults.DefaultBundlePath)
		if err != nil {
			fmt.Printf("failed to get client cert and key: %v\n", err)
			os.Exit(1)
		}
		logger.Debugf("using user cert [%v] and key [%v]", clientCert, clientKey)

		client, err := client.NewClient(logger, quiet, clientCert, clientKey)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		resp, err := client.ServerControl(proto.ServerControlAction_SERVER_CONTROL_DRAIN)
		if err != nil {
			fmt.Printf("server drain request failed: %v\n", err)
			os.Exit(1)
		} else {
			fmt.Printf("successfully started draining conduit server. current state: %s\n", resp.GetServerState())
		}
	},
}

var serverStatusCmd = &cobra.Command{
	Use:    "status",
	Short:  "get conduit server state",
	Long:   `This subcommand will retrieve the current state of the conduit server`,
	Hidden: false,
	Run: func(cmd *cobra.Command, args []string) {
		logger := logrus.New()
		if debug {
			logger.SetLevel(logrus.DebugLevel)
		}

		clientCert := viper.GetString(defaults.ConfigClientCertKey)
		clientKey := viper.GetString(defaults.ConfigClientKeyKey)


		if serverCmdJSON {
			quiet = true
		}

		client, err := client.NewClient(logger, quiet, clientCert, clientKey)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		if serverCmdJSON {
			resp, err := client.ServerControl(proto.ServerControlAction_SERVER_CONTROL_STATUS)
			if err != nil {
				fmt.Printf(`"server_state_error": %q`, err.Error())
				os.Exit(1)
			}


			schedResp, err := client.SchedulerInfo()
			if err != nil {
				fmt.Printf("Failed to fetch scheduler info: %v\n", err)
				os.Exit(1)
			}

			
			mo := protojson.MarshalOptions{
				Multiline:       true,
				EmitUnpopulated: true,
			}

			srBytes, err := mo.Marshal(schedResp)
			if err != nil {
				fmt.Printf("Failed to marshal scheduler info: %v\n", err)
				os.Exit(1)
			} 
			
			out := struct {
				ServerState   string          `json:"server_state"`
				SchedulerInfo json.RawMessage `json:"scheduler_info"`
			}{
				ServerState:   resp.GetServerState().String(),
				SchedulerInfo: srBytes,
			}

			data, err := json.MarshalIndent(out, "", "  ")
			if err != nil {
				fmt.Printf("failed to marshal json: %v", err)
				os.Exit(1)
			}

			fmt.Println(string(data))

		} else {

			fmt.Println("Conduit Server")
			fmt.Println("──────────────")

			resp, err := client.ServerControl(proto.ServerControlAction_SERVER_CONTROL_STATUS)
			if err != nil {
				fmt.Printf("State        "+ColorRed+"server status request failed: %v\n"+ColorReset, err)
				os.Exit(1)
			}
			
			serv_state := resp.GetServerState()
			msgColor := ColorizeServerState(serv_state)
			fmt.Printf("State        "+msgColor+"%s\n"+ColorReset, serv_state)

			schedResp, err := client.SchedulerInfo()
			if err != nil {
				fmt.Printf("Schedulers   "+ColorRed+"server scheduler request failed: %v\n"+ColorReset, err)
				os.Exit(1)
			}

			schedulers := schedResp.GetSchedulers()
			fmt.Printf("Schedulers   %d\n", len(schedulers))

			termWidth := goterm.Width()

			if serverCmdVerbose {
				fmt.Println("")

				separator := CalculateSeparatorWidth(termWidth, 100)

				for schedID, schedStatus := range schedulers {
					if termWidth >= 100 {

						fmt.Printf("\nScheduler "+ColorBlue+"%s\n"+ColorReset, schedID)
						fmt.Println(separator)
						fmt.Printf("Queue Size   %d\n\n", schedStatus.GetQueueSize())

						fmt.Println(ColorBold + "NODE                                 JOBS     AVAILABLE MEMORY     JOBS VERSION     RUNNER VERSION" + ColorReset)
						for nodeID, nodeInfo := range schedStatus.Nodes {
							fmt.Printf("%-36.36s %-8d %-7.2f GiB           %-15d %.*s\n", nodeID, len(nodeInfo.GetJobs()), (float64(nodeInfo.GetAvailableMemory()))/(1024*1024*1024), nodeInfo.GetJobsVersion(), termWidth-84,nodeInfo.GetRunnerVersion())
						}

						fmt.Println("\nJobs")
						fmt.Println(separator)
						fmt.Println(ColorBold + "NODE                                 JOB ID" + ColorReset)
						for nodeID, nodeInfo := range schedStatus.Nodes {
							for jobID := range nodeInfo.GetJobs() {
								fmt.Printf("%-36.36s %s\n", nodeID, jobID)
							}
						}

						fmt.Println("\nQueue")
						fmt.Println(separator)
						fmt.Println(ColorBold + "POSITION                             TRANSFER / JOB" + ColorReset)
						for pos, job := range schedStatus.GetQueue() {
							fmt.Printf("%-35d  %s\n", pos+1, job.GetJobID())
						}

						fmt.Printf("\n\n")

					} else if termWidth >= 36 {
						fmt.Printf("\nScheduler "+ColorBlue+"%s\n"+ColorReset, schedID)
						fmt.Println(separator)
						fmt.Printf("Queue Size   %d\n\n", schedStatus.GetQueueSize())
						
						for nodeID, nodeInfo := range schedStatus.Nodes {
							fmt.Println(ColorBold + "NODE" + ColorReset)
							fmt.Printf("%s\n", nodeID)
							fmt.Println(ColorBold + "JOBS           AVAILABLE MEMORY" + ColorReset)
							fmt.Printf("%-15d%.2f GiB\n", len(nodeInfo.GetJobs()), float64(nodeInfo.GetAvailableMemory())/(1024*1024*1024))
							fmt.Println(ColorBold + "JOBS VERSION   RUNNER VERSION" + ColorReset)
							fmt.Printf("%-15d%.*s\n", nodeInfo.GetJobsVersion(), termWidth-15, nodeInfo.GetRunnerVersion())
							fmt.Println()
						}

						fmt.Println("\nQueue")
						fmt.Println(separator)
						fmt.Println(ColorBold + "POS   TRANSFER/JOB" + ColorReset)
						for pos, job := range schedStatus.GetQueue() {
							fmt.Printf("%-5d %s\n", pos+1, job.GetJobID())
						}
						fmt.Printf("\n\n")

						fmt.Println("\nJobs")
						fmt.Println(separator)
						for nodeID, nodeInfo := range schedStatus.Nodes {
							if len(nodeInfo.GetJobs()) == 0 {
								continue
							}
							fmt.Println(ColorBold + "NODE" + ColorReset)
							fmt.Println(nodeID)
							fmt.Println(ColorBold + "JOB ID" + ColorReset)
							for jobID := range nodeInfo.GetJobs() {
								fmt.Println(jobID)
							}
							fmt.Println()
						}

					} else {
						fmt.Printf("\nScheduler "+ColorBlue+"%s\n"+ColorReset, schedID)
						fmt.Println(separator)
						fmt.Printf("Queue Size   %d\n\n", schedStatus.GetQueueSize())

						for nodeID, nodeInfo := range schedStatus.Nodes {
							fmt.Println(ColorBold + "NODE" + ColorReset)
							fmt.Printf("%s\n", nodeID)
							fmt.Println(ColorBold + "JOBS" + ColorReset)
							fmt.Printf("%d\n", len(nodeInfo.GetJobs()))
							fmt.Println(ColorBold + "AVAILABLE MEMORY" + ColorReset)
							fmt.Printf("%.2f GiB\n", (float64(nodeInfo.GetAvailableMemory()))/(1024*1024*1024))
							fmt.Println(ColorBold + "JOBS VERSION" + ColorReset)
							fmt.Printf("%d\n", nodeInfo.GetJobsVersion())
							fmt.Println(ColorBold + "RUNNER VERSION" + ColorReset)
							fmt.Printf("%.*s\n", termWidth, nodeInfo.GetRunnerVersion())
							fmt.Println()
						}

						fmt.Println("\nJobs")
						fmt.Println(separator)
						for nodeID, nodeInfo := range schedStatus.Nodes {
							//skip nodes with no jobs
							if len(nodeInfo.GetJobs()) == 0 {
								continue
							}

							fmt.Println(ColorBold + "NODE" + ColorReset)
							fmt.Printf("%s\n", nodeID)
							fmt.Println(ColorBold + "JOB ID" + ColorReset)
							for jobID := range nodeInfo.GetJobs() {
								fmt.Printf("%s\n", jobID)
							}
							//blank line between nodes
							fmt.Println()
						}

						fmt.Println("\nQueue")
						fmt.Println(separator)
						for pos, job := range schedStatus.GetQueue() {
							fmt.Println(ColorBold + "POSITION" + ColorReset)
							fmt.Printf("%d\n", pos+1)
							fmt.Println(ColorBold + "TRANSFER / JOB" + ColorReset)
							fmt.Printf("%s\n", job.GetJobID())
							fmt.Println()
						}
						fmt.Printf("\n\n")
					}
				}
			} else {

				separator := CalculateSeparatorWidth(termWidth, 80)

				if termWidth >= 67{
					fmt.Println("\n\nSchedulers")
					fmt.Println(separator)
					fmt.Println(ColorBold + "SCHEDULER     NODES    JOBS    QUEUED" + ColorReset)
					for schedID, schedStatus := range schedulers {
						schedJobs := 0
						for _, nodeInfo := range schedStatus.Nodes {
							schedJobs += len(nodeInfo.GetJobs())
						}
						fmt.Printf("%.8s      %-3d      %-3d     %d\n", schedID, len(schedStatus.Nodes), schedJobs, schedStatus.GetQueueSize())
					}

					fmt.Println("\n\nNodes")
					fmt.Println(separator)
					fmt.Println(ColorBold + "NODE                   JOBS     AVAILABLE MEMORY     RUNNER VERSION" + ColorReset)
					for _, schedStatus := range schedulers {
						for nodeID, nodeInfo := range schedStatus.Nodes {
							fmt.Printf("%-16.16s       %-3d      %-6.2f GiB           %.*s\n", nodeID, len(nodeInfo.GetJobs()), (float64(nodeInfo.GetAvailableMemory()))/(1024*1024*1024), termWidth-53,nodeInfo.GetRunnerVersion())
						}
					}
				} else if termWidth >= 48 {
					fmt.Println("\n\nSchedulers")
					fmt.Println(separator)
					fmt.Println(ColorBold + "SCHEDULER     NODES    JOBS    QUEUED" + ColorReset)
					for schedID, schedStatus := range schedulers {
						schedJobs := 0
						for _, nodeInfo := range schedStatus.Nodes {
							schedJobs += len(nodeInfo.GetJobs())
						}
						fmt.Printf("%.8s      %-3d      %-3d     %d\n", schedID, len(schedStatus.Nodes), schedJobs, schedStatus.GetQueueSize())
					}

					fmt.Println("\n\nNodes")
					fmt.Println(separator)
					for _, schedStatus := range schedulers {
						for nodeID, nodeInfo := range schedStatus.Nodes {
							fmt.Println(ColorBold + "NODE                   JOBS     AVAILABLE MEMORY" + ColorReset)
							fmt.Printf("%-16.16s       %-3d      %-6.2f GiB\n", nodeID, len(nodeInfo.GetJobs()), (float64(nodeInfo.GetAvailableMemory()))/(1024*1024*1024))
							fmt.Println(ColorBold + "                       RUNNER VERSION" + ColorReset)
							fmt.Printf("                       %.*s\n", termWidth-23,nodeInfo.GetRunnerVersion())
							fmt.Println()
						}
					}
				} else {
					fmt.Println("\n\nSchedulers")
					fmt.Println(separator)
					for schedID, schedStatus := range schedulers {
						schedJobs := 0
						for _, nodeInfo := range schedStatus.Nodes {
							schedJobs += len(nodeInfo.GetJobs())
						}
						fmt.Println(ColorBold + "SCHEDULER" + ColorReset)
						fmt.Printf("%.*s\n", min(termWidth, 8), schedID)
						fmt.Println(ColorBold + "NODES" + ColorReset)
						fmt.Printf("%d\n", len(schedStatus.Nodes))
						fmt.Println(ColorBold + "JOBS" + ColorReset)
						fmt.Printf("%d\n", schedJobs)
						fmt.Println(ColorBold + "QUEUED" + ColorReset)
						fmt.Printf("%d\n", schedStatus.GetQueueSize())
						fmt.Println()
					}

					fmt.Println("\n\nNodes")
					fmt.Println(separator)
					for _, schedStatus := range schedulers {
						for nodeID, nodeInfo := range schedStatus.Nodes {
							fmt.Println(ColorBold + "NODE" + ColorReset)
							fmt.Printf("%.*s\n", min(termWidth, 16), nodeID)
							fmt.Println(ColorBold + "JOBS" + ColorReset)
							fmt.Printf("%d\n", len(nodeInfo.GetJobs()))
							fmt.Println(ColorBold + "AVAILABLE MEMORY" + ColorReset)
							fmt.Printf("%.2f GiB\n", (float64(nodeInfo.GetAvailableMemory()))/(1024*1024*1024))
							fmt.Println(ColorBold + "RUNNER VERSION" + ColorReset)
							fmt.Printf("%.*s\n", termWidth,nodeInfo.GetRunnerVersion())
							fmt.Println()
						}
					}
				}
			}
		}
	},
}

func init() {
	RootCmd.AddCommand(serverCmd)
	serverCmd.AddCommand(serverDrainCmd)
	serverCmd.AddCommand(serverStartCmd)
	serverCmd.AddCommand(serverStatusCmd)

	serverStatusCmd.Flags().BoolVar(&serverCmdVerbose, "verbose", false, "Display verbose information about the conduit server state")
	serverStatusCmd.Flags().BoolVar(&serverCmdJSON, "json", false, "Display information in JSON format")
}
