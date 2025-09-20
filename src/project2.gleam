import argv
import gleam/erlang/process.{type Subject, receive, send_after}
import gleam/float
import gleam/int
import gleam/io
import gleam/list
import gleam/otp/actor
import gleam/pair
import gleam/result
import gleam/time/duration
import gleam/time/timestamp

pub fn main() -> Nil {
  let assert Ok(args) = list.rest(argv.load().arguments)
  let len = list.length(args)
  case len {
    3 -> {
      //get num of nodes
      let assert Ok(num_string) = list.first(args)
      let assert Ok(n) = int.parse(num_string)
      //throw away num nodes and get topology
      let assert Ok(args) = list.rest(args)
      let assert Ok(topology) = list.first(args)
      //throw away topology and get alogorithm
      let assert Ok(args) = list.rest(args)
      let assert Ok(algorithm) = list.first(args)
      //set up monitor
      let reply_subject = process.new_subject()

      //round n to nearest perfect cube if necessary
      let n = case topology {
        "3D" -> get_perfect_cube(n)
        "imp3D" -> get_perfect_cube(n)
        _ -> n
      }

      let finish_num = float.round(int.to_float(n) *. 0.95)

      let monitor_state = MonitorState(0, finish_num, reply_subject)
      let assert Ok(monitor) =
        actor.new(monitor_state)
        |> actor.on_message(monitor_handle_message)
        |> actor.start

      //set up actors
      let empty_actors = []

      let actors =
        start_workers(n, topology, algorithm, empty_actors, monitor.data)

      io.println("topology created, time starts now")
      let time_start = timestamp.system_time()
      //start actors
      let random_actor = rand_neighbor_subj(actors)
      case algorithm {
        "gossip" -> {
          actor.send(random_actor, GossipStart(8.0))
        }
        "push-sum" -> {
          actor.send(random_actor, PushSumStart)
        }
        _ -> {
          io.println("Invalid algorithm")
        }
      }
      case receive(reply_subject, 50_000) {
        // timeout in ms
        Ok(time_end) -> {
          let duration = timestamp.difference(time_start, time_end)
          io.println(float.to_string(duration.to_seconds(duration)))
        }
        Error(_) -> io.println("Monitor timeout")
      }
    }
    _ -> io.println("Wrong number of arguments")
  }
}

///tracker
pub type MonitorMessage {
  Update(index: Int)
}

pub type MonitorState {
  MonitorState(
    count: Int,
    // number of actors that have reached convergence
    total: Int,
    // total number of actors to wait for
    reply_to: Subject(timestamp.Timestamp),
    // main process to notify when done
  )
}

fn monitor_handle_message(
  state: MonitorState,
  message: MonitorMessage,
) -> actor.Next(MonitorState, MonitorMessage) {
  case message {
    Update(_index) -> {
      let new_count = state.count + 1
      //io.println("node " <> int.to_string(index) <> " completed!")
      io.println(int.to_string(new_count) <> " nodes converged")
      case new_count == state.total {
        True -> {
          // all actors have converged, notify main process
          let now = timestamp.system_time()
          io.println("Reached convergence! Time stops now.")
          actor.send(state.reply_to, now)
          actor.stop()
        }
        False -> {
          // keep waiting for more updates
          actor.continue(MonitorState(new_count, state.total, state.reply_to))
        }
      }
    }
  }
}

///worker message def
pub type Message {
  PushSum(sum: Float, weight: Float)
  Gossip(rumor: Float)
  ContactsSetUp(List(#(Int, Subject(Message))), List(Subject(Message)))
  PushSumStart
  GossipStart(rumor: Float)
  PushSumTick
  GossipTick
}

pub type State {
  State(
    //rumor for gossip, sum for push-sum
    val1: Float,
    //num times heard for gossip, weight for push-sum
    val2: Float,
    //only used for push-sum, number of times ratio is unchanged
    val3: Int,
    //list of node's neighbors
    neighbors: List(#(Int, Subject(Message))),
    //where to send convergence notification
    monitor: Subject(MonitorMessage),
    //index of self
    index: Int,
    //sum/weight ratio from previous round, used in push-sum
    prev_ratio: Float,
    //where to send messages to self
    self: List(Subject(Message)),
  )
}

///define the start fucntion for when a worker is messaaged
/// when a work receives the start meesage it starts calculations
fn worker_handle_message(
  state: State,
  message: Message,
) -> actor.Next(State, Message) {
  case message {
    PushSum(sum, weight) -> {
      //receiving push sum message
      //take in new values
      let new_sum = state.val1 +. sum
      let new_weight = state.val2 +. weight
      //update state
      let new_state =
        State(
          new_sum,
          new_weight,
          state.val3,
          state.neighbors,
          state.monitor,
          state.index,
          state.prev_ratio,
          state.self,
        )

      //if it is your first message, start a tick for yourself
      case state.prev_ratio {
        0.0 -> {
          let assert Ok(self) = list.first(state.self)
          send_after(self, 1, PushSumTick)
          Nil
        }
        _ -> Nil
      }
      //continue
      actor.continue(new_state)
    }
    Gossip(rumor) -> {
      //receiving gossip rumor
      case state.val2 <=. 0.0 {
        //last time receiving rumor
        True -> {
          actor.stop()
        }
        False -> {
          //first time receiving rumor
          case state.val1 == 0.0 {
            True -> {
              //let monitor know you heard it
              actor.send(state.monitor, Update(state.index))
              //io.println(int.to_string(state.index) <> " sent to monitor")
              //set up your own ticks
              let assert Ok(self) = list.first(state.self)
              send_after(self, 1, GossipTick)
              Nil
            }
            //not first time or last, no special action
            False -> Nil
          }
          //continue with new value, decrementing the number of times left until quitting
          let new_val = state.val2 -. 1.0
          let new_state =
            State(
              rumor,
              new_val,
              0,
              state.neighbors,
              state.monitor,
              state.index,
              state.prev_ratio,
              state.self,
            )
          //continue 
          actor.continue(new_state)
        }
      }
    }
    ContactsSetUp(neighbors, self) -> {
      //receive list of neighbors and subject for self
      let new_state =
        State(
          state.val1,
          state.val2,
          0,
          neighbors,
          state.monitor,
          state.index,
          state.prev_ratio,
          self,
        )
      actor.continue(new_state)
    }
    PushSumStart -> {
      //starting push-sum algorithm
      let assert Ok(self) = list.first(state.self)
      //send the first tick, indicating the first round is starting
      actor.send(self, PushSumTick)
      //continue as you are
      actor.continue(state)
    }
    GossipStart(rumor) -> {
      //starting gossip algorithm
      let assert Ok(self) = list.first(state.self)
      //set state with rumor value and decrement count until stopping
      let new_state =
        State(
          rumor,
          state.val2 -. 1.0,
          0,
          state.neighbors,
          state.monitor,
          state.index,
          state.prev_ratio,
          state.self,
        )
      //start first round via tick
      actor.send(self, GossipTick)
      //continue with updates state
      actor.continue(new_state)
    }
    PushSumTick -> {
      //tick represents the start/end of a round
      //get current half values
      let halved_sum = state.val1 /. 2.0
      let halved_weight = state.val2 /. 2.0

      //send vals to neighbor
      let random = rand_neighbor_subj(state.neighbors)
      actor.send(random, PushSum(halved_sum, halved_weight))

      //calculate new ratio
      let new_ratio = halved_sum /. halved_weight

      //check convergence 
      let num_repeats = case
        float.absolute_value(state.prev_ratio -. new_ratio) <=. 1.0e-10
      {
        True -> {
          //if no change, update count 
          state.val3 + 1
        }
        False -> {
          //if changed, reset count
          0
        }
      }

      case num_repeats >= 3 {
        //if no change for three rounds, tell monitor and stop
        True -> {
          actor.send(state.monitor, Update(state.index))
          actor.stop()
        }
        False -> {
          //if not converged, set new tick for next round
          let assert Ok(subject) = list.first(state.self)
          send_after(subject, 1, PushSumTick)

          //set new state with halved values, new repeat count, and new ratio
          let new_state =
            State(
              halved_sum,
              halved_weight,
              num_repeats,
              state.neighbors,
              state.monitor,
              state.index,
              new_ratio,
              state.self,
            )
          actor.continue(new_state)
        }
      }
    }
    GossipTick -> {
      //pick neighbor, pass rumor along with your id
      io.println("tick for " <> int.to_string(state.index))
      let neighbor = rand_neighbor_subj(state.neighbors)
      actor.send(neighbor, Gossip(state.val1))

      //set up next tick
      let assert Ok(self) = list.first(state.self)
      send_after(self, 1, GossipTick)
      actor.continue(state)
    }
  }
}

pub fn build_state(n: Int, algorithm: String, monitor: Subject(MonitorMessage)) {
  case algorithm {
    "gossip" -> {
      State(0.0, 10.0, 0, [], monitor, n, 0.0, [])
      //val1 represents rumor, val2 = num times node will receive rumor
    }
    "push-sum" -> {
      let n_float = int.to_float(n)

      State(n_float, 1.0, 0, [], monitor, n, 0.0, [])
      //val1 = sum, val2 = weight
    }
    _ -> {
      io.println("invalid algorithm input")
      State(0.0, 0.0, 0, [], monitor, n, 0.0, [])
    }
  }
}

pub fn start_workers(
  n: Int,
  topology: String,
  algorithm: String,
  workers: List(#(Int, Subject(Message))),
  monitor: Subject(MonitorMessage),
) -> List(#(Int, Subject(Message))) {
  case n > 0 {
    True -> {
      //initial state will depend on the algorithm
      let initial_state = build_state(n, algorithm, monitor)

      let assert Ok(actor) =
        //set up actor
        actor.new(initial_state)
        |> actor.on_message(worker_handle_message)
        |> actor.start

      //add new actor to list
      let new_workers = list.append(workers, [#(n, actor.data)])
      //recurse until n actors have been made
      start_workers(n - 1, topology, algorithm, new_workers, monitor)
    }
    False -> {
      //once all actors have been created, set up topology
      assign_neighbors(list.length(workers), topology, workers)
      workers
    }
  }
}

pub fn assign_neighbors(
  n: Int,
  topology: String,
  actors: List(#(Int, Subject(Message))),
) {
  case n {
    //if n=0, we are done, else treat the current actor
    0 -> Nil
    _ -> {
      //get nth actor
      let assert Ok(result) = list.find(actors, fn(x) { pair.first(x) == n })
      let subject = pair.second(result)
      //send neighbors to actor based on topology
      case topology {
        "full" -> {
          //actor gets every actor as neighbor except itself
          actor.send(
            subject,
            ContactsSetUp(list.filter(actors, fn(x) { x.0 != n }), [subject]),
          )
        }
        "3D" -> {
          let neighbors = get_3d_neighbors(n, actors)
          actor.send(subject, ContactsSetUp(neighbors, [subject]))
        }
        "line" -> {
          actor.send(
            subject,
            ContactsSetUp(
              list.filter(actors, fn(x) { x.0 == n + 1 || x.0 == n - 1 }),
              [subject],
            ),
          )
        }
        "imp3D" -> {
          let neighbors = get_imp3d_neighbors(n, actors)
          actor.send(subject, ContactsSetUp(neighbors, [subject]))
        }
        _ -> io.println("invalid topology input")
      }

      //recurse with next neighbor
      assign_neighbors(n - 1, topology, actors)
    }
  }
}

pub fn get_3d_neighbors(
  n: Int,
  actors: List(#(Int, Subject(Message))),
) -> List(#(Int, Subject(Message))) {
  //get coordinates of node in matrix
  let num_actors = list.length(actors)
  let grid_size = float.power(int.to_float(num_actors), 1.0 /. 3.0)
  let grid_int = float.round(result.unwrap(grid_size, 0.0))
  let #(x, y, z) = num_to_coords(n, grid_int)

  //find neighbors in each direction
  let candidates = [
    #(x - 1, y, z),
    #(x + 1, y, z),
    #(x, y - 1, z),
    #(x, y + 1, z),
    #(x, y, z - 1),
    #(x, y, z + 1),
  ]

  let neighbors =
    list.filter_map(candidates, fn(x) {
      let #(nx, ny, nz) = x
      //filter out invalid coordinates
      case
        nx >= 0
        && ny >= 0
        && nz >= 0
        && nx < grid_int
        && ny < grid_int
        && nz < grid_int
      {
        True -> {
          //find the neighbor's index
          let num = coords_to_num(x, grid_int)
          case list.find(actors, fn(x) { pair.first(x) == num }) {
            //neighbor gets added to neighbors list
            Ok(actor) -> Ok(actor)
            Error(_) -> Error(Nil)
          }
        }
        False -> Error(Nil)
      }
    })
  neighbors
}

pub fn get_imp3d_neighbors(
  n: Int,
  actors: List(#(Int, Subject(Message))),
) -> List(#(Int, Subject(Message))) {
  //get the regular 3d grid neighbors
  let reg_neighbors = get_3d_neighbors(n, actors)
  //get list of other options to add
  let self = list.find(actors, fn(x) { pair.first(x) == n })

  let others =
    list.filter(actors, fn(actor) {
      //any actor not already in neighbors list
      !list.contains(reg_neighbors, actor) && self != Ok(actor)
    })
  //pick a random candidate to add
  let rando = rand_neighbor(others)

  list.append(reg_neighbors, [rando])
}

pub fn num_to_coords(n: Int, size: Int) -> #(Int, Int, Int) {
  //convert n to coordinates in 3d grid
  let x = { n - 1 } % size
  let y = { { n - 1 } / size } % size
  let z = { n - 1 } / { size * size }

  #(x, y, z)
}

pub fn coords_to_num(coords: #(Int, Int, Int), size: Int) -> Int {
  //convert coordinates in grid to node number
  let #(x, y, z) = coords
  1 + x + y * size + z * size * size
}

pub fn get_perfect_cube(n: Int) -> Int {
  //take cube root of n
  let cube_root = float.power(int.to_float(n), 1.0 /. 3.0)
  //round up to nearest whole number
  let whole_cube_root = float.ceiling(result.unwrap(cube_root, 0.0))
  //cube the new root to get a perfect cube value for n
  let perf_cube =
    float.round(result.unwrap(float.power(whole_cube_root, 3.0), 0.0))
  perf_cube
}

pub fn rand_neighbor_subj(
  list: List(#(Int, Subject(Message))),
) -> process.Subject(Message) {
  let size = list.length(list)
  let rando = int.random(size)
  let neighbor = find_neighbor(list, rando, 0)
  neighbor.1
}

pub fn rand_neighbor(
  list: List(#(Int, Subject(Message))),
) -> #(Int, Subject(Message)) {
  let size = list.length(list)
  let rando = int.random(size)
  let neighbor = find_neighbor(list, rando, 0)
  neighbor
}

pub fn find_neighbor(
  list: List(#(Int, Subject(Message))),
  goal: Int,
  index: Int,
) -> #(Int, Subject(Message)) {
  case index == goal {
    True -> {
      //Target is at front of the list
      let assert Ok(result) = list.first(list)
      result
    }
    False -> {
      //target is deeper in the list, reove first 
      let assert Ok(new_list) = list.rest(list)
      let new_index = index + 1
      find_neighbor(new_list, goal, new_index)
    }
  }
}
