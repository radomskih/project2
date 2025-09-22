# project2

Team Members: Patriel Stapleton and Helen Radomski

# Max Network Sizes and Times

Gossip Algorithm (0.95 Convergence)	
Full Topology   9300
3D Topology     2100
Line Topology	1900
Imperfect 3D	2100

Push-Sum Algorithm (1.0 Convergence)
Full Topology	12500
3D Topology	    60000
Line Topology	20
Imperfect 3D 	1500


# What is Working
All the alrogithms have been implemented as outlined in the project specifications.
The gossip algorithm only performs efficently at 0.95 convergence thereshold.
We go into more detail in the report.

# What isn't Working
The line and imperfect 3D topologies don't always reach convergence during the push-sum algorithm with repeated num 3.
We wanted to use a seeded random number generator but the gleam stats library threw an error.