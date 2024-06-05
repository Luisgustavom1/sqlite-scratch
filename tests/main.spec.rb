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

  it 'prints error message when table is full' do
    script = (1..1401).map do |i|
      "insert #{i} user_#{i} person#{i}@example.com"
    end
    script << ".exit"
    result = run_script(script)
    expect(result[-2]).to eq("db > Error: table is full")
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
      "db > [CONSTANTS]",
      "ROW_SIZE: 293",
      "COMMON_NODE_HEADER_SIZE: 6",
      "LEAF_NODE_HEADER_SIZE: 10",
      "LEAF_NODE_CELL_SIZE: 297",
      "LEAF_NODE_SPACE_FOR_CELLS: 4086",
      "LEAF_NODE_MAX_CELLS: 13",
      "db > ",
    ])
  end
end
