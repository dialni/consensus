How to run the program
1. Create 3 terminals instances and navigate them to the Node folder
2. Run "go run node.go" in each instance. The first node active will determine when the program starts.
3. Open a terminal and navigate to CriticalZone, and type "go run CriticalZone.go"
4. When all 3 terminals are ready, press enter in the terminal that activated first.
5. The program will now simulate 3 active nodes attempting to access the same resource.
6. If a request is timed out or dropped eventually, for any reason, then the program must be restarted. This is a problem related to the Ricart-Argawala algorithm not handling any of this by default.