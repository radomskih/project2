# project2

Team Members: Patriel Stapleton and Helen Radomski

# Max Network Sizes and Times
Note: Topology set-up generally took much longer than convergence algorithms and provided the limit on size.

Gossip Algorithm (0.95 Convergence)	
Toplogy         Nodes (N)   Time (s)
Full Topology   10,000      30.53
3D Topology     100,000     13.29
Line Topology   1,000       9.63
Imperfect 3D	10,000      1.99

Push-Sum Algorithm (0.95 Convergence)
Toplogy         Nodes (N)   Time (s)
Full Topology	10,000      8.04
3D Topology	    60,000      4.15
Line Topology	100         1.24
Imperfect 3D 	60,000      4.28


# What is Working
All the alrogithms have been implemented as outlined in the project specifications.
The gossip algorithm only performs reliably at partial convergence thereshold (~95%).
We go into more detail in the report.

# What isn't Working
The line topology is unreliable and often does not finish for high values of n.