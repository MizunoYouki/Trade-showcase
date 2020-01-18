from datetime import datetime


def log_to_pine_shape(location):
    """
    plotshape(time == timestamp(2018, 12, 20, 17, 0) ? 465200 : na, style=shape.cross, transp=0, location=location.absolute)
    plotshape(time == timestamp(2018, 12, 20, 17, 0) ? 466200 : na, style=shape.xcross, transp=0, location=location.absolute)
    plotshape(time == timestamp(2018, 12, 20, 17, 0), text='Hello', color=#ffccff, textcolor=black, style=shape.labeldown)
    """

    def extract_datetime(raw):
        at = ':'.join(raw.split(':')[:2])
        return datetime.strptime(at, '%Y-%m-%dT%H:%M')

    with open(location) as fd:
        for line in fd:
            line = line.strip()
            # print(line)
            if 'open asset' in line:
                e = line.split(',')

                entry = e[1].strip().split('entry:')[1].split('.0')[0]
                entry = int(entry)
                raw_at = e[2].split('at:')[1]
                at = extract_datetime(raw_at)
                # print(at)

                side = e[0].split('(')[1]
                if side == 'Position.Long':
                    style = 'shape.triangleup'
                elif side == 'Position.Short':
                    style = 'shape.triangledown'
                else:
                    raise Exception('Unexpected side: {}'.format(side))

                print(
                    'plotshape(time == timestamp({}, {}, {}, {}, {}) ? {} : na, '
                    'style={}, transp=66, location=location.absolute, color=#9900ff)'.format(
                        at.year, at.month, at.day, at.hour, at.minute, entry, style
                    )
                )

            elif 'close asset' in line:
                e = line.split(',')

                _exit = e[5].strip().split('exit:')[1].split('.0')[0]
                _exit = int(_exit)

                profit = e[1].strip().split('profit:')[1].split('.0')[0]
                profit = int(profit)
                raw_at = e[0].split('at:')[1]
                at = extract_datetime(raw_at)
                # print(at)

                print(
                    'plotshape(time == timestamp({}, {}, {}, {}, {}) ? {}: na, '
                    'style=shape.xcross, transp=66, location=location.absolute, color=#9900ff)'.format(
                        at.year, at.month, at.day, at.hour, at.minute, _exit
                    )
                )
                if 0 <= profit:
                    print(
                        'plotshape(time == timestamp({}, {}, {}, {}, {}'
                        '), text="{}/{}\\n{: }", '
                        'color=#9999cc, textcolor=color.black, style=shape.labeldown)'.format(
                            at.year, at.month, at.day, at.hour, at.minute, entry, _exit, profit
                        )
                    )
                else:
                    print(
                        'plotshape(time == timestamp({}, {}, {}, {}, {}), text="{}/{}\\n{: }", '
                        'color=#ffccff, textcolor=color.black, style=shape.labeldown)'.format(
                            at.year, at.month, at.day, at.hour, at.minute, entry, _exit, profit
                        )
                    )

            if not line:
                continue


if __name__ == '__main__':
    log_to_pine_shape('C:/Users/mizun/PycharmProjects/Trade/1min-your-special-strategy.log')
