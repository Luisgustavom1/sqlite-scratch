describe 'database' do
  before do
    `rm -rf ./tests/test.db`
  end

  after(:all) do 
    `rm -rf ./tests/test.db`
  end

  def run_script(commands)
    output = nil
    IO.popen("./sqlite ./tests/test.db", "r+") do |pipe|
      commands.each do |command|
        pipe.puts command
      end

      pipe.close_write

      output = pipe.gets(nil)
    end
    output.split("\n")
  end

  it 'inserts and retrieves a row' do
    result = run_script([
      "insert 1 user_1 user_1@example.com",
      "select",
      ".exit",
    ])
    expect(result).to match_array([
      "db > ", 
      "db > (1, user_1, user_1@example.com)", 
      "db > executed", 
      "executed"
    ])
  end

  it 'allows inserting strings that are the maximum length' do
    long_username = "a"*32
    long_email = "a"*255
    script = [
      "insert 1 #{long_username} #{long_email}",
      "select",
      ".exit",
    ]
    result = run_script(script)
    expect(result).to match_array([
      "db > ",
      "db > (1, #{long_username}, #{long_email})",
      "db > executed",
      "executed",
    ])
  end

  it 'prints error message when not pass all args to insert statement' do
    script = [
       "insert first_arg",
       ".exit",
    ]
    result = run_script(script)
    expect(result).to match_array([
      "db > ",
      "db > Syntax error. Could not parse statement.",
    ])
  end
  
  it 'prints error message if strings are too long' do
    long_username = "a"*33
    long_email = "a"*256
    script = [
       "insert 1 #{long_username} #{long_email}",
       "select",
       ".exit",
    ]
    result = run_script(script)
    expect(result).to match_array([
      "db > ",
      "db > string is too long",
      "db > executed",
    ])
  end
  
  it 'prints an error message if id is negative' do    
    script = [
      "insert -1 username foo@bar.com",
      "select",
      ".exit",
    ]
    result = run_script(script)
    expect(result).to match_array([
      "db > ",
      "db > ID must be positive",
      "db > executed",
    ])
  end

  
  it 'inserts and retrieves a row after closing connection' do
    result = run_script([
      "insert 1 user_1 user_1@example.com",
      ".exit",
    ])
    expect(result).to match_array([
      "db > ", 
      "db > executed", 
    ])
    
    result2 = run_script([
      "select",
      ".exit",
    ])
    expect(result2).to match_array([
      "db > ",
      "db > (1, user_1, user_1@example.com)",
      "executed",
    ])
  end

  it 'prints constants' do
    script = [
      ".constants",
      ".exit",
    ]
    result = run_script(script)

    expect(result).to match_array([
      "db > Constants ->",
      "ROW_SIZE: 293",
      "COMMON_NODE_HEADER_SIZE: 6",
      "LEAF_NODE_HEADER_SIZE: 14",
      "LEAF_NODE_CELL_SIZE: 297",
      "LEAF_NODE_SPACE_FOR_CELLS: 4082",
      "LEAF_NODE_MAX_CELLS: 13",
      "db > ",
    ])
  end

  xit 'print out the structure of a one-node btree' do
    commands = [3, 1, 2].map do |i|
      "insert #{i} user#{i} person#{i}@example.com"
    end
    commands << ".btree"
    commands << ".exit"

    result = run_script(commands);

    expect(result).to match_array([
      "db > executed",
      "db > executed",
      "db > executed",
      "db > Btree ->",
      "- leaf (size 3)",
      " - 1",
      " - 2",
      " - 3",
      "db > "
    ])
  end
  
  it 'print out the structure of a 3-leaf-node btree' do
    commands = (1..14).map do |i|
      "insert #{i} user#{i} person#{i}@example.com"
    end
    commands << ".btree"
    commands << "insert 15 user15 person15@example.com"
    commands << ".exit"
    
    result = run_script(commands)

    expect(result[14...(result.length)]).to match_array([
      "db > Btree ->",
      "- internal (size 1)",
      " - leaf (size 7)",
      "  - 1",
      "  - 2",
      "  - 3",
      "  - 4",
      "  - 5",
      "  - 6",
      "  - 7",
      " - key 7",
      " - leaf (size 7)",
      "  - 8",
      "  - 9",
      "  - 10",
      "  - 11",
      "  - 12",
      "  - 13",
      "  - 14",
      "db > executed",
      "db > ",
    ])
  end

  it 'prints all rows in a multi-level tree' do
    commands = (1..15).map do |i|
      "insert #{i} user#{i} person#{i}@example.com"
    end
    commands << "select"
    commands << ".exit"
    
    result = run_script(commands)

    expect(result[15...result.length]).to match_array([
      "db > (1, user1, person1@example.com)",
      "(2, user2, person2@example.com)",
      "(3, user3, person3@example.com)",
      "(4, user4, person4@example.com)",
      "(5, user5, person5@example.com)",
      "(6, user6, person6@example.com)",
      "(7, user7, person7@example.com)",
      "(8, user8, person8@example.com)",
      "(9, user9, person9@example.com)",
      "(10, user10, person10@example.com)",
      "(11, user11, person11@example.com)",
      "(12, user12, person12@example.com)",
      "(13, user13, person13@example.com)",
      "(14, user14, person14@example.com)",
      "(15, user15, person15@example.com)",
      "executed", 
      "db > ",
    ])
  end

  it 'prints an error message if there is a duplicate id' do
    script = [
      "insert 1 user1 person1@example.com",
      "insert 1 user1 person1@example.com",
      "select",
      ".exit",
    ]
    result = run_script(script)
    expect(result).to match_array([
      "db > executed",
      "db > Error: duplicate key",
      "db > (1, user1, person1@example.com)",
      "executed",
      "db > ",
    ])
  end
end
