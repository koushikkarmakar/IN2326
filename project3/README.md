## Bonus Project 3

The third bonus project is about searching in social graphs. For a description of the exact task, please see the attached PDF file.

Please find source code in this repository: https://gitlab.db.in.tum.de/kersten/fde18-bonusproject3 . Fork this project if you want to work on the task.

As a first step, please set the visibility to private. Go to Settings -> Permissions and set Project visibility to Private. If you don't do this, everyone can see your implementation and use your tricks.
As a second step, please set your teamname and realname in team.txt. The teamname is used in a public leaderboard, realname will be hidden and only used to provide the bonus.

Submission

The repository is set up so that every commit that is pushed to gitlab is picked up by gitlabs continuous integration system. It will first run tests. If tests pass, a performance evaluations stage is started. The timing result of this stage is submitted to contest.db.in.tum.de. All submissions which occurred in this way and execute in less than 20 seconds will be considered for the bonus. You are welcome to submit early and often. The runtime in the leaderboard will be the minimum runtime of all your submissions.

Deadline

8.2.18 23:59

All submissions before this date will be considered for the bonus.

Bonus System

There will be three projects, set up similarly to this one. If you complete all of them, you will receive a grade bonus of 0.3 on you final exam grade.

The workload for these tasks is not very high. So I ask you to work on these projects alone. There will be no teams of multiple people allowed for submission. However, you are welcome to discuss your solutions and approaches.

FAQ

"I get a 'wrong result' error, but my code works on my machine"

Make sure you open the input file in read mode (not in write mode). E.g. use std::ifstream instead of std::fstream.

"How many queries are in the benchmark on the server?"

50

"Can I change all files in the repository?"

Yes of course, just make sure that you read from stdin and write to stdout as the template version does.

"Any tips on what to optimize?"

Do the usual:

    Make sure the implemented algorithm is reasonably efficient. In this case, BFS or how about bidirectional BFS? (The latter usually discovers much fewer nodes than a one directional BFS)
    Check how much time is spent on which part of your program. Use the provided playedin.csv and run 50 (random) queries on it. Use perf record or measure time spent from within your program. This will show you which parts of the program to optimize next.
    Possible issues: Too many memory allocations. Not all resources used (12 Threads are available on the server)

Besides that:

Do the parsing of playedin.csv in a single thread.

Use multiple threads to run queries. (std::async is your friend)

Instead of using unordered_map<Actor, unordered_set<Movie>> use a vector<vector<Movie>>. That allows for much faster access.

Do not use std::istringstream. This one is very slow. Instead, you can use std::iftream and the extract operator >>
