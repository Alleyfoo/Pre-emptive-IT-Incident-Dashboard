param(
  [Parameter(ValueFromRemainingArguments = $true)]
  [string[]]$Args
)

python "$PSScriptRoot\data_agents_cli.py" @Args
