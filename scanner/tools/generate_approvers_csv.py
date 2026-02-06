"""
生成审批人名单CSV
================
生成只有一列人名的CSV，按权重分配

使用示例:
  uv run -m scanner.tools.generate_approvers_csv 100
"""

import argparse
import csv
import random
from datetime import datetime
from pathlib import Path

DEFAULT_APPROVERS = ["邹子扬", "谢文敏", "吴镔", "向伟", "向文杰", "王炜龙", "李虹霖", "刘涛萌", "张浩春"]


def prompt_approvers() -> dict:
    """交互式选择审批人"""
    print("\n可选审批人:")
    for i, name in enumerate(DEFAULT_APPROVERS, 1):
        print(f"  {i}. {name}")
    print("-" * 40)
    
    while True:
        user_input = input("选择审批人 (如 1,2,3): ").strip()
        if not user_input:
            return {}
        
        try:
            selected = {}
            for idx_str in user_input.split(","):
                idx = int(idx_str.strip())
                if 1 <= idx <= len(DEFAULT_APPROVERS):
                    selected[DEFAULT_APPROVERS[idx - 1]] = 1
            
            if not selected:
                continue
            
            print(f"已选: {', '.join(selected.keys())}")
            
            if input("自定义权重? (y/N): ").strip().lower() in ['y', 'yes']:
                for name in selected:
                    w = input(f"  {name}: ").strip()
                    if w:
                        selected[name] = int(w)
            
            total = sum(selected.values())
            print("\n分配:")
            for name, w in selected.items():
                print(f"  {name}: {w/total*100:.1f}%")
            
            if input("确认? (Y/n): ").strip().lower() in ['', 'y', 'yes']:
                return selected
        except ValueError:
            print("请输入数字")


def generate(num_rows: int, config: dict) -> list:
    """按权重生成人名列表"""
    names = list(config.keys())
    weights = list(config.values())
    total = sum(weights)
    
    counts = [int(num_rows * w / total) for w in weights]
    remainder = num_rows - sum(counts)
    for i in range(remainder):
        counts[i % len(counts)] += 1
    
    result = []
    for name, count in zip(names, counts):
        result.extend([name] * count)
    random.shuffle(result)
    return result


def main():
    parser = argparse.ArgumentParser(description="生成审批人名单CSV")
    parser.add_argument("-o", "--output", help="输出文件")
    args = parser.parse_args()
    
    # 交互式输入行数
    while True:
        count_input = input("生成多少行: ").strip()
        try:
            count = int(count_input)
            if count > 0:
                break
            print("请输入正整数")
        except ValueError:
            print("请输入数字")
    
    config = prompt_approvers()
    if not config:
        return
    
    names = generate(count, config)
    
    output = args.output or f"ExportedCSV/approvers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    Path(output).parent.mkdir(exist_ok=True)
    
    with open(output, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        for name in names:
            writer.writerow([name])
    
    print(f"\n✓ 已生成: {output}")
    print(f"  共 {len(names)} 行")


if __name__ == "__main__":
    main()
