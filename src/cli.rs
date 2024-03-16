use std::fmt::{Display, Formatter};

#[derive(Clone, Debug)]
pub enum Role {
    Slave(String),
    Master(String),
}

impl Display for Role {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Role::Master(s) => write!(f, "master"),
            Role::Slave(s) => write!(f, "slave"),
        }
    }
}

#[derive(Debug)]
pub struct CLI {
    pub port: String,
    pub master_server: String,
    pub role: Role,
}

impl CLI {
    pub fn new(args: Vec<String>) -> Self {
        print!("Command line arguments are: {:?}", args);
        let mut port: String = String::from("127.0.0.1:");
        let mut master_server: String = String::new();
        let role: Role;
        match args.get(1) {
            Some(_port_flag) => {
                let port_number = args.get(2).unwrap();
                port.push_str(port_number.as_str());
            }
            None => port.push_str("6379"),
        }
        match args.get(3) {
            Some(_replica_of_flag) => {
                let master_host = args.get(4).to_owned().unwrap();
                let master_port = args.get(5).to_owned().unwrap();
                master_server = format!("{}:{}", master_host, master_port);
                role = Role::Slave(String::from("slave"));
            }
            None => role = Role::Master(String::from("master")),
        }
        Self {
            port,
            master_server,
            role,
        }
    }
}
